package sql

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/metadata"

	"github.com/hellofresh/goengine"
)

// AggregateProjector is a postgres projector used to execute a projection per aggregate instance against an event stream
type AggregateProjector struct {
	sync.Mutex

	backgroundProcessor *ProjectionNotificationProcessor
	executor            *notificationProjector
	storage             AggregateProjectorStorage

	projectionErrorHandler ProjectionErrorCallback

	db *sql.DB

	logger goengine.Logger
}

// NewAggregateProjector creates a new projector for a projection
func NewAggregateProjector(
	db *sql.DB,
	eventLoader EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	projection goengine.Projection,
	projectorStorage AggregateProjectorStorage,
	projectionErrorHandler ProjectionErrorCallback,
	logger goengine.Logger,
	metrics Metrics,
	retryDelay time.Duration,
) (*AggregateProjector, error) {
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

	processor, err := NewBackgroundProcessor(10, 32, logger, metrics, nil)
	if err != nil {
		return nil, err
	}

	executor, err := NewNotificationProjector(
		db,
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

	return &AggregateProjector{
		backgroundProcessor:    processor,
		executor:               executor,
		storage:                projectorStorage,
		projectionErrorHandler: projectionErrorHandler,

		db: db,

		logger: logger,
	}, nil
}

// Run executes the projection and manages the state of the projection
func (a *AggregateProjector) Run(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	return a.backgroundProcessor.Execute(ctx, a.processNotification, nil)
}

// RunAndListen executes the projection and listens to any changes to the event store
func (a *AggregateProjector) RunAndListen(ctx context.Context, listener Listener) error {
	a.Lock()
	defer a.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	stopExecutor := a.backgroundProcessor.Start(ctx, a.processNotification)
	defer stopExecutor()

	return listener.Listen(ctx, a.backgroundProcessor.Queue)
}

func (a *AggregateProjector) processNotification(
	ctx context.Context,
	notification *ProjectionNotification,
	queue ProjectionTrigger,
) error {
	var (
		err       error
		logFields func(e goengine.LoggerEntry)
	)
	if notification != nil {
		err = a.executor.Execute(ctx, notification)
		logFields = func(e goengine.LoggerEntry) {
			e.Error(err)
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		}
	} else {
		err = a.triggerOutOfSyncProjections(ctx, queue)
		logFields = func(e goengine.LoggerEntry) {
			e.Error(err)
		}
	}

	// No error occurred during projection so return
	if err == nil {
		return nil
	}

	// Resolve the action to take based on the error that occurred
	switch resolveErrorAction(a.projectionErrorHandler, notification, err) {
	case errorFail:
		a.logger.Debug("ProcessHandler->ErrorHandler: marking projection as failed", logFields)
		return a.markProjectionAsFailed(notification)
	case errorIgnore:
		a.logger.Debug("ProcessHandler->ErrorHandler: ignoring error", logFields)
		return nil
	case errorRetry:
		a.logger.Debug("ProcessHandler->ErrorHandler: re-queueing notification", logFields)
		return queue(ctx, notification)
	}

	a.logger.Debug("ProcessHandler->ErrorHandler: error fallthrough", logFields)
	return err
}

func (a *AggregateProjector) triggerOutOfSyncProjections(ctx context.Context, queue ProjectionTrigger) error {
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

		if err := queue(ctx, notification); err != nil {
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

func (a *AggregateProjector) markProjectionAsFailed(notification *ProjectionNotification) error {
	ctx := context.Background()
	conn, err := AcquireConn(ctx, a.db)
	if err != nil {
		return err
	}

	defer func() {
		if err := conn.Close(); err != nil {
			a.logger.Warn("failed to db close failure connection", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
		}
	}()

	return a.storage.PersistFailure(conn, notification)
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
