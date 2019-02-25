package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	internalSQL "github.com/hellofresh/goengine/driver/sql/internal"
	"github.com/pkg/errors"
)

// StreamProjector is a postgres projector used to execute a projection against an event stream.
type StreamProjector struct {
	sync.Mutex
	executor *internalSQL.NotificationProjector

	db *sql.DB

	projectionName  string
	projectionTable string

	logger                 goengine.Logger
	projectionErrorHandler driverSQL.ProjectionErrorCallback
}

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	db *sql.DB,
	eventStore driverSQL.ReadOnlyEventStore,
	resolver goengine.MessagePayloadResolver,
	projection goengine.Projection,
	projectionTable string,
	projectionErrorHandler driverSQL.ProjectionErrorCallback,
	logger goengine.Logger,
) (*StreamProjector, error) {
	switch {
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case eventStore == nil:
		return nil, goengine.InvalidArgumentError("eventStore")
	case resolver == nil:
		return nil, goengine.InvalidArgumentError("resolver")
	case projection == nil:
		return nil, goengine.InvalidArgumentError("projection")
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case projectionErrorHandler == nil:
		return nil, goengine.InvalidArgumentError("projectionErrorHandler")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}
	logger = logger.WithField("projection", projection)

	var (
		stateDecoder driverSQL.ProjectionStateDecoder
		stateEncoder driverSQL.ProjectionStateEncoder
	)
	if saga, ok := projection.(goengine.ProjectionSaga); ok {
		stateDecoder = saga.DecodeState
		stateEncoder = saga.EncodeState
	}

	storage, err := newStreamProjectionStorage(projection.Name(), projectionTable, stateEncoder, logger)
	if err != nil {
		return nil, err
	}

	executor, err := internalSQL.NewNotificationProjector(
		db,
		storage,
		stateDecoder,
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

		db: db,

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

	return s.processNotification(ctx, nil)
}

// RunAndListen executes the projection and listens to any changes to the event store
func (s *StreamProjector) RunAndListen(ctx context.Context, listener driverSQL.Listener) error {
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

	return listener.Listen(ctx, s.processNotification)
}

func (s *StreamProjector) processNotification(
	ctx context.Context,
	notification *driverSQL.ProjectionNotification,
) error {
	for i := 0; i < math.MaxInt16; i++ {
		err := s.executor.Execute(ctx, notification)

		// No error occurred during projection so return
		if err == nil {
			return err
		}

		// Resolve the action to take based on the error that occurred
		logger := s.logger.WithError(err).WithField("notification", notification)
		switch resolveErrorAction(s.projectionErrorHandler, notification, err) {
		case errorRetry:
			logger.Debug("Trigger->ErrorHandler: retrying notification")
			continue
		case errorIgnore:
			logger.Debug("Trigger->ErrorHandler: ignoring error")
			return nil
		case errorFail, errorFallthrough:
			logger.Debug("Trigger->ErrorHandler: error fallthrough")
			return err
		}
	}

	return errors.Errorf(
		"seriously %d retries is enough! maybe it's time to fix your projection or error handling code?",
		math.MaxInt16,
	)
}

// setupProjection Creates the projection if none exists
func (s *StreamProjector) setupProjection(ctx context.Context) error {
	// Ignore duplicate inserts
	_, err := s.db.ExecContext(
		ctx,
		/* #nosec */
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1) ON CONFLICT DO NOTHING`,
			QuoteIdentifier(s.projectionTable),
		),
		s.projectionName,
	)

	return err
}
