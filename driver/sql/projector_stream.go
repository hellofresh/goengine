package sql

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
)

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	db *sql.DB,
	eventLoader EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	projection goengine.Projection,
	projectorStorage StreamProjectorStorage,
	projectionErrorHandler ProjectionErrorCallback,
	logger goengine.Logger,
) (ProjectionTrigger, error) {
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

	projector, err := NewNotificationProjector(
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

	return NewNotificationStreamErrorHandler(projector, projectionErrorHandler, logger)
}

// NewNotificationAggregateErrorHandler returns a ProjectionTrigger that will check the result of next and handle the errors
func NewNotificationStreamErrorHandler(
	next ProjectionTrigger,
	projectionErrorHandler ProjectionErrorCallback,
	logger goengine.Logger,
) (ProjectionTrigger, error) {
	switch {
	case next == nil:
		return nil, goengine.InvalidArgumentError("next")
	case projectionErrorHandler == nil:
		return nil, goengine.InvalidArgumentError("projectionErrorHandler")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}

	return func(ctx context.Context, notification *ProjectionNotification) error {
		err := next(ctx, notification)
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
		switch resolveErrorAction(projectionErrorHandler, notification, err) {
		case errorRetry:
			logger.Debug("Trigger->ErrorHandler: retrying notification", logFields)
			return next(ctx, notification)
		case errorIgnore:
			logger.Debug("Trigger->ErrorHandler: ignoring error", logFields)
			return nil
		default:
			logger.Debug("Trigger->ErrorHandler: error fallthrough", logFields)
			return err
		}
	}, nil
}

// StreamProjectionEventStreamLoader returns a EventStreamLoader for the StreamProjector
func StreamProjectionEventStreamLoader(eventStore ReadOnlyEventStore, streamName goengine.StreamName) EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification, position int64) (goengine.EventStream, error) {
		return eventStore.LoadWithConnection(ctx, conn, streamName, position+1, nil, nil)
	}
}
