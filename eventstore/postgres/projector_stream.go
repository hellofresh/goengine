package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/internal/log"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	// ErrProjectionFailedToLock occurs when the projector cannot acquire the projection lock
	ErrProjectionFailedToLock = errors.New("unable to acquire projection lock")
	// Ensure that we satisfy the eventstore.Projector interface
	_ eventstore.Projector = &StreamProjector{}
)

type (
	// StreamProjector is a postgres projector used to execute a projection against an event stream.
	//
	// This projector uses postgres advisory locks (https://www.postgresql.org/docs/10/static/explicit-locking.html#ADVISORY-LOCKS)
	// to avoid projecting the same event multiple times.
	// Updates to the event stream are received by using the postgres notify and listen.
	StreamProjector struct {
		sync.Mutex

		dbDSN           string
		store           eventstore.EventStore
		resolver        eventstore.PayloadResolver
		projection      eventstore.Projection
		projectionTable string
		logger          logrus.FieldLogger

		eventHandlers map[string]eventstore.ProjectionHandler

		db       *sql.DB
		state    interface{}
		position int64
	}

	// projectorNotification is a representation of the data provided by postgres notify
	projectorNotification struct {
		No          int64  `json:"no"`
		EventName   string `json:"event_name"`
		AggregateID string `json:"aggregate_id"`
	}
)

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	dbDSN string,
	store eventstore.EventStore,
	resolver eventstore.PayloadResolver,
	projection eventstore.Projection,
	projectionTable string,
	logger logrus.FieldLogger,
) (*StreamProjector, error) {
	if logger == nil {
		logger = log.NilLogger
	}
	logger = logger.WithFields(logrus.Fields{
		"projection":   projection.Name(),
		"event_stream": projection.FromStream(),
	})

	return &StreamProjector{
		dbDSN:           dbDSN,
		store:           store,
		resolver:        resolver,
		projection:      projection,
		projectionTable: projectionTable,
		logger:          logger,
	}, nil
}

// StreamProjectorCreateSchema return the sql statement needed for the postgres database in order to use the StreamProjector
func StreamProjectorCreateSchema(projectionTable string, streamName eventstore.StreamName, streamTable string) []string {
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
		pq.QuoteIdentifier(projectionTable),
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
		pq.QuoteIdentifier(triggerName),
		pq.QuoteIdentifier(streamTable),
		quoteString(string(streamName)),
	)

	return statements
}

// Run executes the projection and manages the state of the projection
func (s *StreamProjector) Run(ctx context.Context, keepRunning bool) error {
	s.Lock()
	defer s.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	// Open a database
	if err := s.dbOpen(); err != nil {
		return err
	}
	defer s.dbClose()

	// Create the projection if none exists
	if !s.projectionExists(ctx) {
		if err := s.createProjection(ctx); err != nil {
			return err
		}
	}

	// Trigger an initial run of the projection
	if err := s.triggerRun(ctx); err != nil {
		// If the projector needs to keep running but could not acquire a lock we still need to continue.
		if err == ErrProjectionFailedToLock && !keepRunning {
			return err
		}
	}
	if !keepRunning {
		return nil
	}

	listener := pq.NewListener(s.dbDSN, time.Millisecond, time.Second, func(event pq.ListenerEventType, err error) {
		logger := s.logger.WithField("listener_event", event)
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
			s.dbClose()
		case pq.ListenerEventReconnected:
			logger.Debug("connection listener: reconnected")
		default:
			logger.Warn("connection listener: unknown event")
		}
	})
	defer listener.Close()

	if err := listener.Listen(string(s.projection.FromStream())); err != nil {
		return err
	}

	for {
		select {
		case n := <-listener.Notify:
			s.dbOpen()

			logger := s.logger
			if n == nil {
				logger.Warn("received nil notification")
			} else {
				logger := logger.WithFields(logrus.Fields{
					"channel":    n.Channel,
					"data":       n.Extra,
					"process_id": n.BePid,
				})

				// If extra data is provided use it to check if the projection needs to catch up
				if n.Extra != "" {
					// decode the extra's
					var notification projectorNotification
					if err := json.Unmarshal([]byte(n.Extra), &notification); err != nil {
						logger.Warn("received notification with a invalid data")
					} else if notification.No <= s.position {
						// The current position is a head of the notification so ignore
						logger.Debug("ignoring notification: it is behind the projection position")
						continue
					}

					logger.Debug("received notification")
				} else {
					logger.Warn("received notification without data")
				}
			}

			if err := s.triggerRun(ctx); err != nil {
				if err == ErrProjectionFailedToLock {
					logger.WithError(err).Info("ignoring notification: the projection is already locked")
					continue
				}

				return err
			}
		case <-ctx.Done():
			s.logger.Debug("context closed stopping projection")
			return nil
		}
	}
}

// Reset trigger a reset of the projection and the projections state
func (s *StreamProjector) Reset(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	return s.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := s.projection.Reset(ctx); err != nil {
			return err
		}

		s.position = 0
		s.state = nil

		return s.persist(ctx, conn)
	})
}

// Delete removes the projection and the projections state
func (s *StreamProjector) Delete(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	return s.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := s.projection.Delete(ctx); err != nil {
			return err
		}

		_, err := s.db.ExecContext(
			ctx,
			fmt.Sprintf(
				`DELETE FROM %s WHERE name = $1`,
				pq.QuoteIdentifier(s.projectionTable),
			),
			s.projection.Name(),
		)

		if err != nil {
			return err
		}

		return nil
	})
}

func (s *StreamProjector) triggerRun(ctx context.Context) error {
	streamName := s.projection.FromStream()

	return s.do(ctx, func(ctx context.Context, conn *sql.Conn) error {
		stream, err := s.store.Load(ctx, streamName, s.position+1, nil, nil)
		if err != nil {
			return err
		}

		if err := s.handleStream(ctx, conn, streamName, stream); err != nil {
			if err := stream.Close(); err != nil {
				s.logger.WithError(err).Warn("failed to close the stream after a handling error occurred")
			}
			return err
		}

		if err := stream.Close(); err != nil {
			return err
		}

		return nil
	})
}

func (s *StreamProjector) handleStream(ctx context.Context, conn *sql.Conn, streamName eventstore.StreamName, stream eventstore.EventStream) error {
	var msgCount int64
	for stream.Next() {
		// Check if the context is expired
		select {
		default:
		case <-ctx.Done():
			return nil
		}

		// Get the message
		msg, msgNumber, err := stream.Message()
		if err != nil {
			return err
		}
		msgCount++
		s.position = msgNumber

		// Resolve the payload event name
		eventName, err := s.resolver.ResolveName(msg.Payload())
		if err != nil {
			s.logger.WithField("payload", msg.Payload()).Debug("skipping event: unable to resolve payload name")
			continue
		}

		// Load event handlers if needed
		if s.eventHandlers == nil {
			s.eventHandlers = s.projection.Handlers()
		}

		// Resolve the payload handler using the event name
		handler, found := s.eventHandlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		s.state, err = handler(ctx, s.state, msg)
		if err != nil {
			return err
		}

		// Persist state and position changes
		if err := s.persist(ctx, conn); err != nil {
			return err
		}
	}

	return stream.Err()
}

func (s *StreamProjector) do(ctx context.Context, callback func(ctx context.Context, conn *sql.Conn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := s.acquireProjection(ctx, conn); err != nil {
		return err
	}
	defer s.releaseProjection(ctx, conn)

	return callback(ctx, conn)
}

func (s *StreamProjector) acquireProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), position, state FROM %s WHERE name = $1`,
			quoteString(s.projectionTable),
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
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
		return ErrProjectionFailedToLock
	}

	var err error
	s.state, err = s.projection.ReconstituteState(jsonState)
	if err != nil {
		return err
	}
	s.position = position

	return nil
}

func (s *StreamProjector) releaseProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_advisory_unlock(%s::regclass::oid::int, no) FROM %s WHERE name = $1`,
			quoteString(s.projectionTable),
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
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

func (s *StreamProjector) persist(ctx context.Context, conn *sql.Conn) error {
	jsonState, err := json.Marshal(s.state)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`UPDATE %s SET position = $1, state = $2 WHERE name = $3`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.position,
		jsonState,
		s.projection.Name(),
	)
	if err != nil {
		return err
	}
	s.logger.WithField("position", s.position).Debug("updated projection state")

	return nil
}

func (s *StreamProjector) projectionExists(ctx context.Context) bool {
	rows, err := s.db.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT 1 FROM %s WHERE name = $1 LIMIT 1`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)
	if err != nil {
		s.logger.WithField("table", s.projectionTable).WithError(err).Error("failed to query projection table")
		return false
	}
	defer rows.Close()

	if !rows.Next() {
		return false
	}

	var found bool
	if err := rows.Scan(&found); err != nil {
		s.logger.WithField("table", s.projectionTable).WithError(err).Error("failed to scan projection table")
		return false
	}

	return found
}

func (s *StreamProjector) createProjection(ctx context.Context) error {
	// Ignore duplicate inserts. This can occur when multiple projectors are started at the same time.
	_, err := s.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1) ON CONFLICT DO NOTHING`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *StreamProjector) dbOpen() error {
	var err error
	if s.db == nil {
		s.logger.Debug("opening db connection")
		s.db, err = sql.Open("postgres", s.dbDSN)
	}

	return err
}

func (s *StreamProjector) dbClose() {
	if s.db == nil {
		return
	}

	db := s.db
	s.db = nil

	if err := db.Close(); err != nil {
		s.logger.WithError(err).Debug("failed to close db connection")
	}
}
