package sql

import (
	"context"
	"database/sql"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/metadata"
)

// NewAggregateProjector creates a new projector for a projection
func NewAggregateProjector(
	db *sql.DB,
	queue NotificationQueuer,
	eventLoader EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	projection goengine.Projection,
	projectorStorage AggregateProjectorStorage,
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

	projector, err = NewNotificationAggregateErrorHandler(
		projector,
		projectionErrorHandler,
		queue.ReQueue,
		func(ctx context.Context, notification *ProjectionNotification) error {
			return projectorStorage.PersistFailure(db, notification)
		},
		logger,
	)
	if err != nil {
		return nil, err
	}

	return NewNotificationAggregateSyncHandler(projector, db, projectorStorage, queue.Queue, logger)
}

// aggregateSyncProjector Handles nil notification for the Aggregate Projections
type aggregateSyncProjector struct {
	next    ProjectionTrigger
	db      *sql.DB
	storage AggregateProjectorStorage
	queue   ProjectionTrigger
	logger  goengine.Logger
}

// NewNotificationAggregateSyncHandler returns a Projection trigger that will when a nil notification is received
// find all the missed events and send them to the queue
func NewNotificationAggregateSyncHandler(
	next ProjectionTrigger,
	db *sql.DB,
	storage AggregateProjectorStorage,
	queue ProjectionTrigger,
	logger goengine.Logger,
) (ProjectionTrigger, error) {
	switch {
	case next == nil:
		return nil, goengine.InvalidArgumentError("next")
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case storage == nil:
		return nil, goengine.InvalidArgumentError("storage")
	case queue == nil:
		return nil, goengine.InvalidArgumentError("queue")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	projector := aggregateSyncProjector{
		next:    next,
		db:      db,
		storage: storage,
		queue:   queue,
		logger:  logger,
	}
	return projector.Handle, nil
}

// Handle if notification is nil find the missed notifications and queue them
func (a *aggregateSyncProjector) Handle(ctx context.Context, notification *ProjectionNotification) error {
	if notification != nil {
		return a.next(ctx, notification)
	}

	// A nil notification was received this mean that we need to find and trigger any missed notifications
	conn, err := AcquireConn(ctx, a.db)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			a.logger.Warn("failed to db close LoadOutOfSync connection", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
		}
	}()

	rows, err := a.storage.LoadOutOfSync(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			a.logger.Error("failed to close LoadOutOfSync rows", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
		}
	}()

	for rows.Next() {
		// Check if the context is expired
		select {
		default:
		case <-ctx.Done():
			return nil
		}

		var (
			aggregateID string
			position    int64
		)

		if err := rows.Scan(&aggregateID, &position); err != nil {
			return err
		}

		notification := &ProjectionNotification{
			No:          position,
			AggregateID: aggregateID,
		}

		if err := a.queue(ctx, notification); err != nil {
			a.logger.Error("failed to queue notification", func(e goengine.LoggerEntry) {
				e.Error(err)
				e.Int64("notification.no", notification.No)
				e.String("notification.aggregate_id", notification.AggregateID)
			})
			return err
		}

		a.logger.Debug("send catchup", func(e goengine.LoggerEntry) {
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		})
	}

	return rows.Close()
}

// NewNotificationAggregateErrorHandler returns a ProjectionTrigger that will check the result of next and handle the errors
func NewNotificationAggregateErrorHandler(
	next ProjectionTrigger,
	projectionErrorHandler ProjectionErrorCallback,
	retry ProjectionTrigger,
	fail ProjectionTrigger,
	logger goengine.Logger,
) (ProjectionTrigger, error) {
	switch {
	case next == nil:
		return nil, goengine.InvalidArgumentError("next")
	case projectionErrorHandler == nil:
		return nil, goengine.InvalidArgumentError("projectionErrorHandler")
	case retry == nil:
		return nil, goengine.InvalidArgumentError("retry")
	case fail == nil:
		return nil, goengine.InvalidArgumentError("fail")
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
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		}

		switch resolveErrorAction(projectionErrorHandler, notification, err) {
		case errorFail:
			logger.Debug("NotificationBrokerErrorHandler: marking projection as failed", logFields)
			return fail(ctx, notification)
		case errorIgnore:
			logger.Debug("NotificationBrokerErrorHandler: ignoring error", logFields)
			return nil
		case errorRetry:
			logger.Debug("NotificationBrokerErrorHandler: re-queueing notification", logFields)
			return retry(ctx, notification)
		}

		logger.Debug("NotificationBrokerErrorHandler: error fallthrough", logFields)
		return err
	}, nil
}

// AggregateProjectionEventStreamLoader returns a EventStreamLoader for the AggregateProjector
func AggregateProjectionEventStreamLoader(eventStore ReadOnlyEventStore, streamName goengine.StreamName, aggregateTypeName string) EventStreamLoader {
	matcher := metadata.NewMatcher()
	matcher = metadata.WithConstraint(matcher, aggregate.TypeKey, metadata.Equals, aggregateTypeName)

	return func(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification, position int64) (goengine.EventStream, error) {
		aggMatcher := metadata.WithConstraint(matcher, aggregate.IDKey, metadata.Equals, notification.AggregateID)
		return eventStore.LoadWithConnection(ctx, conn, streamName, position+1, nil, aggMatcher)
	}
}
