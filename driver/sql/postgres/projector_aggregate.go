package postgres

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/internal"
)

// AggregateProjector is a postgres projector used to execute a projection per aggregate instance against an event stream
type AggregateProjector struct {
	sync.Mutex

	backgroundProcessor *internal.BackgroundProcessor
	executor            *internal.NotificationProjector
	storage             *aggregateProjectionStorage

	projectionErrorHandler driverSQL.ProjectionErrorCallback

	db *sql.DB

	logger goengine.Logger
}

// NewAggregateProjector creates a new projector for a projection
func NewAggregateProjector(
	db *sql.DB,
	eventStore driverSQL.ReadOnlyEventStore,
	eventStoreTable string,
	resolver goengine.MessagePayloadResolver,
	aggregateTypeName string,
	projection goengine.Projection,
	projectionTable string,
	projectionErrorHandler driverSQL.ProjectionErrorCallback,
	logger goengine.Logger,
) (*AggregateProjector, error) {
	switch {
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case eventStore == nil:
		return nil, goengine.InvalidArgumentError("eventStore")
	case strings.TrimSpace(eventStoreTable) == "":
		return nil, goengine.InvalidArgumentError("eventStoreTable")
	case resolver == nil:
		return nil, goengine.InvalidArgumentError("resolver")
	case projection == nil:
		return nil, goengine.InvalidArgumentError("projection")
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case aggregateTypeName == "":
		return nil, goengine.InvalidArgumentError("aggregateTypeName")
	case projectionErrorHandler == nil:
		return nil, goengine.InvalidArgumentError("projectionErrorHandler")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}
	logger = logger.WithFields(func(e goengine.LoggerEntry) {
		e.String("projection", projection.Name())
	})

	processor, err := internal.NewBackgroundProcessor(10, 32, logger)
	if err != nil {
		return nil, err
	}

	var (
		stateDecoder driverSQL.ProjectionStateDecoder
		stateEncoder driverSQL.ProjectionStateEncoder
	)
	if saga, ok := projection.(goengine.ProjectionSaga); ok {
		stateDecoder = saga.DecodeState
		stateEncoder = saga.EncodeState
	}

	storage, err := newAggregateProjectionStorage(eventStoreTable, projectionTable, stateEncoder, logger)
	if err != nil {
		return nil, err
	}

	executor, err := internal.NewNotificationProjector(
		db,
		storage,
		projection.Init,
		stateDecoder,
		projection.Handlers(),
		aggregateProjectionEventStreamLoader(eventStore, projection.FromStream(), aggregateTypeName),
		resolver,
		logger,
	)
	if err != nil {
		return nil, err
	}

	return &AggregateProjector{
		backgroundProcessor:    processor,
		executor:               executor,
		storage:                storage,
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
func (a *AggregateProjector) RunAndListen(ctx context.Context, listener driverSQL.Listener) error {
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
	notification *driverSQL.ProjectionNotification,
	queue driverSQL.ProjectionTrigger,
) error {
	var err error
	if notification != nil {
		err = a.executor.Execute(ctx, notification)
	} else {
		err = a.triggerOutOfSyncProjections(ctx, queue)
	}

	// No error occurred during projection so return
	if err == nil {
		return nil
	}

	// Resolve the action to take based on the error that occurred
	logFields := func(e goengine.LoggerEntry) {
		e.Error(err)
		e.Int64("notification.no", notification.No)
		e.String("notification.aggregate_id", notification.AggregateID)
	}
	switch resolveErrorAction(a.projectionErrorHandler, notification, err) {
	case errorFail:
		a.logger.Debug("ProcessHandler->ErrorHandler: marking projection as failed", logFields)
		return a.markProjectionAsFailed(ctx, notification)
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

func (a *AggregateProjector) triggerOutOfSyncProjections(ctx context.Context, queue driverSQL.ProjectionTrigger) error {
	// A nil notification was received this mean that we need to find and trigger any missed notifications
	conn, err := internal.AcquireConn(ctx, a.db)
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

		notification := &driverSQL.ProjectionNotification{
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

func (a *AggregateProjector) markProjectionAsFailed(ctx context.Context, notification *driverSQL.ProjectionNotification) error {
	conn, err := internal.AcquireConn(ctx, a.db)
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

	return a.storage.PersistFailure(ctx, conn, notification)
}
