package postgres

import (
	"context"
	"database/sql"
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

	db       *sql.DB
	executor *internalSQL.NotificationProjector
	storage  driverSQL.StreamProjectorStorage

	projectionErrorHandler driverSQL.ProjectionErrorCallback

	logger goengine.Logger
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
	logger = logger.WithFields(func(e goengine.LoggerEntry) {
		e.String("projection", projection.Name())
	})

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
		projection.Init,
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
		db:                     db,
		executor:               executor,
		storage:                storage,
		projectionErrorHandler: projectionErrorHandler,
		logger:                 logger,
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

	if err := s.storage.CreateProjection(ctx, s.db); err != nil {
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

	if err := s.storage.CreateProjection(ctx, s.db); err != nil {
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
		logFields := func(e goengine.LoggerEntry) {
			e.Error(err)
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		}
		switch resolveErrorAction(s.projectionErrorHandler, notification, err) {
		case errorRetry:
			s.logger.Debug("Trigger->ErrorHandler: retrying notification", logFields)
			continue
		case errorIgnore:
			s.logger.Debug("Trigger->ErrorHandler: ignoring error", logFields)
			return nil
		case errorFail, errorFallthrough:
			s.logger.Debug("Trigger->ErrorHandler: error fallthrough", logFields)
			return err
		}
	}

	return errors.Errorf(
		"seriously %d retries is enough! maybe it's time to fix your projection or error handling code?",
		math.MaxInt16,
	)
}
