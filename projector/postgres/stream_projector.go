package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/eventstore/postgres"
	eventStoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/log"
	"github.com/hellofresh/goengine/projector"
	"github.com/hellofresh/goengine/projector/internal"
	"github.com/lib/pq"
)

var (
	// ErrNoEventStore occurs when no event store is provided
	ErrNoEventStore = errors.New("no event store provided")
	// ErrNoPayloadResolver occurs when no payload resolver is provided
	ErrNoPayloadResolver = errors.New("no payload resolver provided")
	// ErrNoProjection occurs when no projection is provided
	ErrNoProjection = errors.New("no projection provided")
	// ErrNoProjectionTableName occurs when no projection table name is provided
	ErrNoProjectionTableName = errors.New("no projection table name provided")
	// ErrNoProjectionErrorHandler occurs when no projection error handler func is provided
	ErrNoProjectionErrorHandler = errors.New("no projection error handler provided")

	// Ensure that we satisfy the eventstore.Projector interface
	_ projector.Projector = &StreamProjector{}
)

// StreamProjector is a postgres projector used to execute a projection against an event stream.
type StreamProjector struct {
	sync.Mutex
	executor *internal.NotificationProjector

	db        *sql.DB
	dbDSN     string
	dbChannel string

	projectionName  string
	projectionTable string

	logger                 goengine_dev.Logger
	projectionErrorHandler projector.ProjectionErrorCallback
}

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	dbDSN string,
	db *sql.DB,
	eventStore eventStoreSQL.ReadOnlyEventStore,
	resolver goengine_dev.MessagePayloadResolver,
	projection goengine_dev.Projection,
	projectionTable string,
	projectionErrorHandler projector.ProjectionErrorCallback,
	logger goengine_dev.Logger,
) (*StreamProjector, error) {
	switch {
	case strings.TrimSpace(dbDSN) == "":
		return nil, postgres.ErrNoDBConnect
	case eventStore == nil:
		return nil, ErrNoEventStore
	case resolver == nil:
		return nil, ErrNoPayloadResolver
	case projection == nil:
		return nil, ErrNoProjection
	case strings.TrimSpace(projectionTable) == "":
		return nil, ErrNoProjectionTableName
	case projectionErrorHandler == nil:
		return nil, ErrNoProjectionErrorHandler
	}

	if db == nil {
		var err error
		db, err = sql.Open("postgres", dbDSN)
		if err != nil {
			return nil, err
		}
	}
	if logger == nil {
		logger = log.NilLogger
	}
	logger = logger.WithField("projection", projection)

	executor, err := internal.NewNotificationProjector(
		db,
		newStreamProjectionStorage(projection.Name(), projectionTable, logger),
		projection.ReconstituteState,
		projection.Handlers(),
		streamProjectionEventStreamLoader(eventStore, projection.FromStream()),
		resolver,
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &StreamProjector{
		executor: executor,

		db:        db,
		dbDSN:     dbDSN,
		dbChannel: string(projection.FromStream()),

		projectionName:         projection.Name(),
		projectionTable:        projectionTable,
		projectionErrorHandler: projectionErrorHandler,

		logger: logger,
	}, nil
}

// Run executes the projection and manages the state of the projection
func (s *StreamProjector) Run(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	if err := s.setupProjection(ctx); err != nil {
		return err
	}

	return internal.WrapTriggerWithErrorHandler(
		s.projectionErrorHandler,
		s.executor.Execute,
		s.logger,
	)(ctx, nil)
}

// RunAndListen executes the projection and listens to any changes to the event store
func (s *StreamProjector) RunAndListen(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	if err := s.setupProjection(ctx); err != nil {
		return err
	}

	listener, err := newListener(s.dbDSN, s.dbChannel, s.logger)
	if err != nil {
		return err
	}

	return listener.Listen(ctx, internal.WrapTriggerWithErrorHandler(
		s.projectionErrorHandler,
		s.executor.Execute,
		s.logger,
	))
}

// setupProjection Creates the projection if none exists
func (s *StreamProjector) setupProjection(ctx context.Context) error {
	return internal.ExecOnConn(ctx, s.db, s.logger, func(ctx context.Context, conn *sql.Conn) error {
		if s.projectionExists(ctx, conn) {
			return nil
		}
		if err := s.createProjection(ctx, conn); err != nil {
			return err
		}

		return nil
	})
}

func (s *StreamProjector) projectionExists(ctx context.Context, conn *sql.Conn) bool {
	rows, err := conn.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT 1 FROM %s WHERE name = $1 LIMIT 1`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projectionName,
	)
	if err != nil {
		s.logger.
			WithError(err).
			WithField("table", s.projectionTable).
			Error("failed to query projection table")
		return false
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.logger.
				WithError(err).
				WithField("table", s.projectionTable).
				Warn("failed to close rows")
		}
	}()

	if !rows.Next() {
		return false
	}

	var found bool
	if err := rows.Scan(&found); err != nil {
		s.logger.
			WithError(err).
			WithField("table", s.projectionTable).
			Error("failed to scan projection table")
		return false
	}

	return found
}

func (s *StreamProjector) createProjection(ctx context.Context, conn *sql.Conn) error {
	// Ignore duplicate inserts. This can occur when multiple projectors are started at the same time.
	_, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1) ON CONFLICT DO NOTHING`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projectionName,
	)
	if err != nil {
		return err
	}

	return nil
}
