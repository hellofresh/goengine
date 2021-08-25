package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/hellofresh/goengine"
)

// StreamProjector is a postgres projector used to execute a projection against an event stream.
type StreamProjector struct {
	sync.Mutex

	db       *sql.DB
	executor *notificationProjector
	storage  StreamProjectorStorage

	projectionErrorHandler ProjectionErrorCallback

	logger goengine.Logger
}

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	db *sql.DB,
	eventLoader EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	projection goengine.Projection,
	projectorStorage StreamProjectorStorage,
	projectionErrorHandler ProjectionErrorCallback,
	logger goengine.Logger,
) (*StreamProjector, error) {
	switch {
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case eventLoader == nil:
		return nil, goengine.InvalidArgumentError("eventLoader")
	case resolver == nil:
		return nil, goengine.InvalidArgumentError("resolver")
	case projection == nil:
		return nil, goengine.InvalidArgumentError("projection")
	case projectorStorage == nil:
		return nil, goengine.InvalidArgumentError("projectorStorage")
	case projectionErrorHandler == nil:
		return nil, goengine.InvalidArgumentError("projectionErrorHandler")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}
	logger = logger.WithFields(func(e goengine.LoggerEntry) {
		e.String("projection", projection.Name())
	})

	executor, err := newNotificationProjector(
		db,
		projectorStorage,
		projection.Handlers(),
		eventLoader,
		resolver,
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &StreamProjector{
		db:                     db,
		executor:               executor,
		storage:                projectorStorage,
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
func (s *StreamProjector) RunAndListen(ctx context.Context, listener Listener) error {
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
	notification *ProjectionNotification,
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
			if notification == nil {
				e.Any("notification", notification)
			} else {
				e.Int64("notification.no", notification.No)
				e.String("notification.aggregate_id", notification.AggregateID)
			}
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

	return fmt.Errorf(
		"seriously %d retries is enough! maybe it's time to fix your projection or error handling code",
		math.MaxInt16,
	)
}

// StreamProjectionEventStreamLoader returns a EventStreamLoader for the StreamProjector
func StreamProjectionEventStreamLoader(eventStore ReadOnlyEventStore, streamName goengine.StreamName) EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification, position int64) (goengine.EventStream, error) {
		return eventStore.LoadWithConnection(ctx, conn, streamName, position+1, nil, nil)
	}
}
