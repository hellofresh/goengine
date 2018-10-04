package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/internal/log"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	_ eventstore.Projector = &SingleProjector{}
)

// SingleProjector is a postgres projector used execute a projection again an event stream
//
// This projector uses postgres advisory locks (https://www.postgresql.org/docs/10/static/explicit-locking.html#ADVISORY-LOCKS)
// to avoid projecting the same event multiple times.
// Updates to the event stream are received by using the postgres notify and listen.
type SingleProjector struct {
	db              *sql.DB
	dbDSN           string
	store           eventstore.EventStore
	resolver        eventstore.PayloadResolver
	projection      eventstore.Projection
	projectionTable string
	logger          logrus.FieldLogger

	eventHandlers map[string]eventstore.ProjectionHandler

	state    interface{}
	position int64
}

// NewSingleProjector creates a new projector for a projection
func NewSingleProjector(
	dbDSN string,
	db *sql.DB,
	store eventstore.EventStore,
	resolver eventstore.PayloadResolver,
	projection eventstore.Projection,
	projectionTable string,
	logger logrus.FieldLogger,
) (*SingleProjector, error) {
	if logger == nil {
		logger = log.NilLogger
	}
	logger = logger.WithFields(logrus.Fields{
		"projection":   projection.Name(),
		"event_stream": projection.FromStream(),
	})

	return &SingleProjector{
		db:              db,
		dbDSN:           dbDSN,
		store:           store,
		resolver:        resolver,
		projection:      projection,
		projectionTable: projectionTable,
		logger:          logger,
	}, nil
}

// SingleProjectorCreateSchema return the sql statement needed for the postgres database in order to use the SingleProjector
func SingleProjectorCreateSchema(projectionTable string, streamName eventstore.StreamName, streamTable string) []string {
	statements := make([]string, 3)
	statements[0] = `CREATE FUNCTION public.event_stream_notify ()
  RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
  channel text := TG_ARGV[0];
BEGIN
  PERFORM (
          WITH payload AS
          (
              SELECT NEW.no, NEW.event_name, NEW.metadata -> '_aggregate_id' AS aggregate_id
          )
          SELECT pg_notify(channel, row_to_json(payload)::text) FROM payload
          );
  RETURN NULL;
END;
$$;`

	statements[1] = fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			no SERIAL,
			name VARCHAR(150) NOT NULL,
			position BIGINT NOT NULL DEFAULT 0,
			state JSONB NOT NULL DEFAULT ('{}'),
			PRIMARY KEY (no),
			UNIQUE (name)
		)`,
		quoteIdentifier(projectionTable),
	)

	triggerName := fmt.Sprintf("%s_notify", streamTable)
	statements[2] = fmt.Sprintf(
		`DO LANGUAGE plpgsql $$
		 BEGIN
		   IF NOT EXISTS(
		       SELECT * FROM information_schema.triggers
		       WHERE
		           event_object_table = %s AND
		           trigger_name = %s
		   )
		   THEN
		     CREATE TRIGGER %s
		       AFTER INSERT
		       ON %s
		       FOR EACH ROW
		     EXECUTE PROCEDURE event_stream_notify(%s);
		   END IF;
		 END;
		 $$`,
		quoteString(streamTable),
		quoteString(triggerName),
		quoteIdentifier(triggerName),
		quoteIdentifier(streamTable),
		quoteString(string(streamName)),
	)

	return statements
}

// Run executes the projection and manages the state of the projection
func (a *SingleProjector) Run(ctx context.Context, keepRunning bool) error {
	if !a.projectionExists(ctx) {
		if err := a.createProjection(ctx); err != nil {
			return err
		}
	}

	// Trigger a initial run of the projection
	if err := a.triggerRun(ctx); err != nil {
		return err
	}
	if keepRunning == false {
		return nil
	}

	listener := pq.NewListener(a.dbDSN, time.Millisecond, time.Second, func(event pq.ListenerEventType, err error) {
		logger := a.logger.WithField("listener_event", event)
		if err != nil {
			logger = logger.WithError(err)
		}

		switch event {
		case pq.ListenerEventConnected:
			logger.Debug("connection listener: connected")
		case pq.ListenerEventConnectionAttemptFailed:
			logger.Debug("connection listener: failed to connect")
		case pq.ListenerEventDisconnected:
			logger.Debug("connection listener: disconnected")
		case pq.ListenerEventReconnected:
			logger.Debug("connection listener: reconnected")
		default:
			logger.Warn("connection listener: unknown event")
		}
	})

	if err := listener.Listen(string(a.projection.FromStream())); err != nil {
		return err
	}

	for {
		select {
		case n := <-listener.Notify:
			a.logger.WithField("notification", n).Debug("received database notification")

			// If extra data is provided use it to check if the projection needs to catch up
			if n.Extra != "" {
				// decode the extra's

				// If the no is less then the current position ignore this notification
			}

			a.triggerRun(ctx)
		case <-ctx.Done():
			a.logger.Debug("context closed stopping projection")
			return nil
		}
	}
}

// Reset trigger a reset of the projection and the projections state
func (a *SingleProjector) Reset(ctx context.Context) error {
	return a.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := a.projection.Reset(ctx); err != nil {
			return err
		}

		a.position = 0
		a.state = nil

		return a.persist(ctx, conn)
	})
}

// Delete removes the projection and the projections state
func (a *SingleProjector) Delete(ctx context.Context) error {
	return a.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := a.projection.Delete(ctx); err != nil {
			return err
		}

		res, err := a.db.ExecContext(
			ctx,
			fmt.Sprintf(
				`DELETE FROM %s WHERE name = $1`,
				quoteIdentifier(a.projectionTable),
			),
			a.projection.Name(),
		)

		if err != nil {
			return err
		}

		res.RowsAffected()

		return nil
	})
}

func (a *SingleProjector) triggerRun(ctx context.Context) error {
	streamName := a.projection.FromStream()

	return a.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		stream, err := a.store.Load(ctx, streamName, a.position+1, nil, nil)
		if err != nil {
			return err
		}

		if err := a.handleStream(ctx, conn, streamName, stream); err != nil {
			if err := stream.Close(); err != nil {
				a.logger.WithError(err).Warn("failed to close the stream after a handling error occurred")
			}
			return err
		}

		if err := stream.Close(); err != nil {
			return err
		}

		return nil
	})
}

func (a *SingleProjector) handleStream(ctx context.Context, conn *sql.Conn, streamName eventstore.StreamName, stream eventstore.EventStream) error {
	var msgCount int64
	for stream.Next() {
		// Get the message
		msg, msgNumber, err := stream.Message()
		if err != nil {
			return err
		}
		msgCount++
		a.position = msgNumber

		// Resolve the payload event name
		eventName, err := a.resolver.ResolveName(msg.Payload())
		if err != nil {
			continue
		}

		// Load event handlers if needed
		if a.eventHandlers == nil {
			a.eventHandlers = a.projection.Handlers()
		}

		// Resolve the payload handler using the event name
		handler, found := a.eventHandlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		a.state, err = handler(ctx, a.state, msg)
		if err != nil {
			return err
		}

		// Persist state and position changes
		if err := a.persist(ctx, conn); err != nil {
			return err
		}
	}

	return stream.Err()
}

func (a *SingleProjector) do(ctx context.Context, callback func(ctx context.Context, conn *sql.Conn) error) error {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := a.acquireProjection(ctx, conn); err != nil {
		return err
	}
	defer a.releaseProjection(ctx, conn)

	return callback(ctx, conn)
}

func (a *SingleProjector) acquireProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), position, state FROM %s WHERE name = $1`,
			quoteString(a.projectionTable),
			quoteIdentifier(a.projectionTable),
		),
		a.projection.Name(),
	)

	var (
		locked    bool
		jsonState []byte
		position  int64
	)
	if err := res.Scan(&locked, &position, &jsonState); err != nil {
		return err
	}

	if !locked {
		return errors.New("unable to acquire projection lock")
	}

	var err error
	a.state, err = a.projection.ReconstituteState(jsonState)
	if err != nil {
		return err
	}

	return nil
}

func (a *SingleProjector) releaseProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_advisory_unlock(%s::regclass::oid::int, no) FROM %s WHERE name = $1`,
			quoteString(a.projectionTable),
			quoteIdentifier(a.projectionTable),
		),
		a.projection.Name(),
	)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release projection lock")
	}

	return nil
}

func (a *SingleProjector) persist(ctx context.Context, conn *sql.Conn) error {
	jsonState, err := json.Marshal(a.state)
	if err != nil {
		return err
	}

	res, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`UPDATE %s SET position = $1, state = $2 WHERE name = $3`,
			quoteIdentifier(a.projectionTable),
		),
		a.position,
		jsonState,
		a.projection.Name(),
	)
	if err != nil {
		return err
	}
	res.RowsAffected()

	return nil
}

func (a *SingleProjector) projectionExists(ctx context.Context) bool {
	rows, err := a.db.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT 1 FROM %s WHERE name = $1 LIMIT 1`,
			quoteIdentifier(a.projectionTable),
		),
		a.projection.Name(),
	)
	if err != nil {
		a.logger.WithField("table", a.projectionTable).WithError(err).Error("failed to query projection table")
		return false
	}
	defer rows.Close()

	if !rows.Next() {
		return false
	}

	var found bool
	if err := rows.Scan(&found); err != nil {
		a.logger.WithField("table", a.projectionTable).WithError(err).Error("failed to scan projection table")
		return false
	}

	return found
}

func (a *SingleProjector) createProjection(ctx context.Context) error {
	res, err := a.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1)`,
			quoteIdentifier(a.projectionTable),
		),
		a.projection.Name(),
	)
	if err != nil {
		return err
	}
	res.RowsAffected()

	return nil
}
