package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hellofresh/goengine/eventstore"
	log "github.com/sirupsen/logrus"
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
	store      eventstore.EventStore
	resolver   eventstore.PayloadResolver
	projection eventstore.Projection
	logger     log.FieldLogger

	db          *sql.DB
	table       string
	tableQuoted string

	eventHandlers map[string]eventstore.ProjectionHandler
	state         interface{}
	position      int64
}

// NewSingleProjector creates a new projector for a projection
func NewSingleProjector(
	db *sql.DB,
	projectionTable string,
	store eventstore.EventStore,
	resolver eventstore.PayloadResolver,
	projection eventstore.Projection,
) (*SingleProjector, error) {
	return &SingleProjector{
		store:      store,
		resolver:   resolver,
		projection: projection,

		db:          db,
		table:       projectionTable,
		tableQuoted: quoteIdentifier(projectionTable),

		eventHandlers: projection.Handlers(),
	}, nil
}

// Run executes the projection and manages the state of the projection
func (a *SingleProjector) Run(ctx context.Context, keepRunning bool) error {
	if !a.projectionExists(ctx) {
		if err := a.createProjection(ctx); err != nil {
			return err
		}
	}
	streamName := a.projection.FromStream()

	conn, err := a.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := a.acquireProjection(ctx, conn); err != nil {
		return err
	}

	stream, err := a.store.Load(ctx, streamName, a.position+1, nil, nil)
	if err != nil {
		return err
	}

	if err := a.handleStream(ctx, streamName, stream); err != nil {
		return err
	}

	if err := stream.Close(); err != nil {
		return err
	}

	return a.releaseProjection(ctx, conn)
}

// Reset trigger a reset of the projection and the projections state
func (a *SingleProjector) Reset(ctx context.Context) error {
	if err := a.projection.Reset(ctx); err != nil {
		return err
	}

	a.position = 0
	a.state = nil

	return a.persist(ctx)
}

// Delete removes the projection and the projections state
func (a *SingleProjector) Delete(ctx context.Context) error {
	if err := a.projection.Delete(ctx); err != nil {
		return err
	}

	res, err := a.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`DELETE FROM %s WHERE name = $1`,
			a.tableQuoted,
		),
		a.projection.Name(),
	)

	if err != nil {
		return err
	}

	res.RowsAffected()

	return nil
}

func (a *SingleProjector) acquireProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), position, state FROM %s WHERE name = $1`,
			quoteString(a.table),
			a.tableQuoted,
		),
		a.projection.Name(),
	)

	var (
		locked    bool
		jsonState []byte
		position  int64
	)
	if err := res.Scan(&locked, &position, &jsonState); err != nil {
		fmt.Println("lock", err)
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
			quoteString(a.table),
			a.tableQuoted,
		),
		a.projection.Name(),
	)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		fmt.Println("unlock", err)
		return err
	}

	if !unlocked {
		return errors.New("failed to release projection lock")
	}

	return nil
}

func (a *SingleProjector) projectionExists(ctx context.Context) bool {
	res := a.db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT 1 FROM %s WHERE name = $1`,
			a.tableQuoted,
		),
		a.projection.Name(),
	)

	var found bool
	if err := res.Scan(&found); err != nil {
		fmt.Println("found", err)
		return false
	}

	return found
}

func (a *SingleProjector) createProjection(ctx context.Context) error {
	res, err := a.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				no SERIAL,
				name VARCHAR(150) NOT NULL,
				position BIGINT NOT NULL DEFAULT 0,
				state JSONB NOT NULL DEFAULT ('{}'),
				PRIMARY KEY (no),
				UNIQUE (name)
			)`,
			a.tableQuoted,
		),
	)
	if err != nil {
		fmt.Println("create", err)
		return err
	}
	res.RowsAffected()

	//
	res, err = a.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1)`,
			a.tableQuoted,
		),
		a.projection.Name(),
	)
	if err != nil {
		fmt.Println("insert", err)
		return err
	}
	res.RowsAffected()

	return nil
}

func (a *SingleProjector) handleStream(ctx context.Context, streamName eventstore.StreamName, stream eventstore.EventStream) error {
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
		if err := a.persist(ctx); err != nil {
			return err
		}
	}

	return stream.Err()
}

func (a *SingleProjector) persist(ctx context.Context) error {
	jsonState, err := json.Marshal(a.state)
	if err != nil {
		return err
	}

	res, err := a.db.ExecContext(
		ctx,
		fmt.Sprintf(
			`UPDATE %s SET position = $1, state = $2 WHERE name = $3`,
			a.tableQuoted,
		),
		a.position,
		jsonState,
		a.projection.Name(),
	)
	if err != nil {
		fmt.Println("persist", err)
		return err
	}
	res.RowsAffected()

	return nil
}
