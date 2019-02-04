package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"

	goengine_dev "github.com/hellofresh/goengine-dev"

	"github.com/hellofresh/goengine/eventstore"
	eventStoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/log"
	"github.com/hellofresh/goengine/projector"
	"github.com/hellofresh/goengine/projector/internal"
)

var (
	// ErrNoEventStoreTable occurs when no event store table name is provided
	ErrNoEventStoreTable = errors.New("no event store table provided")
	// ErrEmptyAggregateTypeName occurs when an empty aggregate type name is provided
	ErrEmptyAggregateTypeName = errors.New("empty aggregate type name provided")

	// Ensure that we satisfy the eventstore.Projector interface
	_ projector.Projector = &AggregateProjector{}
)

// AggregateProjector is a postgres projector used to execute a projection per aggregate instance against an event stream
type AggregateProjector struct {
	sync.Mutex

	backgroundProcessor *internal.BackgroundProcessor
	executor            *internal.NotificationProjector
	storage             *aggregateProjectionStorage

	projectionErrorHandler projector.ProjectionErrorCallback

	db        *sql.DB
	dbDSN     string
	dbChannel string

	logger log.Logger
}

// NewAggregateProjector creates a new projector for a projection
func NewAggregateProjector(
	dbDSN string,
	db *sql.DB,
	eventStore eventStoreSQL.ReadOnlyEventStore,
	eventStoreTable string,
	resolver eventstore.MessagePayloadResolver,
	aggregateTypeName string,
	projection goengine_dev.Projection,
	projectionTable string,
	projectionErrorHandler projector.ProjectionErrorCallback,
	logger log.Logger,
) (*AggregateProjector, error) {
	switch {
	case eventStore == nil:
		return nil, ErrNoEventStore
	case strings.TrimSpace(eventStoreTable) == "":
		return nil, ErrNoEventStoreTable
	case resolver == nil:
		return nil, ErrNoPayloadResolver
	case projection == nil:
		return nil, ErrNoProjection
	case strings.TrimSpace(projectionTable) == "":
		return nil, ErrNoProjectionTableName
	case aggregateTypeName == "":
		return nil, ErrEmptyAggregateTypeName
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

	processor, err := internal.NewBackgroundProcessor(10, 32, logger)
	if err != nil {
		return nil, err
	}

	storage := newAggregateProjectionStorage(projectionTable, eventStoreTable, logger)

	executor, err := internal.NewNotificationProjector(
		db,
		storage,
		projection.ReconstituteState,
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

		db:        db,
		dbDSN:     dbDSN,
		dbChannel: string(projection.FromStream()),

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

	// Wrap the processNotification in order to handler error nice and sweet
	handler := internal.WrapProcessHandlerWithErrorHandler(
		a.projectionErrorHandler,
		a.processNotification,
		a.markProjectionAsFailed,
		a.logger,
	)

	return a.backgroundProcessor.Execute(ctx, handler, nil)
}

// RunAndListen executes the projection and listens to any changes to the event store
func (a *AggregateProjector) RunAndListen(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	listener, err := newListener(a.dbDSN, a.dbChannel, a.logger)
	if err != nil {
		return err
	}

	stopExecutor := a.backgroundProcessor.Start(
		ctx,
		internal.WrapProcessHandlerWithErrorHandler(
			a.projectionErrorHandler,
			a.processNotification,
			a.markProjectionAsFailed,
			a.logger,
		),
	)
	defer stopExecutor()

	return listener.Listen(ctx, a.backgroundProcessor.Queue)
}

func (a *AggregateProjector) processNotification(ctx context.Context, notification *projector.Notification, queue internal.Trigger) error {
	if notification != nil {
		return a.executor.Execute(ctx, notification)
	}

	// A nil notification was received this mean that we need to find and trigger any missed notifications
	conn, err := internal.AcquireConn(ctx, a.db)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			a.logger.WithError(err).Warn("failed to db close LoadOutOfSync connection")
		}
	}()

	return a.triggerOutOfSyncProjections(ctx, conn, queue)
}

func (a *AggregateProjector) triggerOutOfSyncProjections(ctx context.Context, conn *sql.Conn, queue internal.Trigger) error {
	rows, err := a.storage.LoadOutOfSync(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			a.logger.WithError(err).Error("failed to close LoadOutOfSync rows")
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

		notification := &projector.Notification{
			No:          position,
			AggregateID: aggregateID,
		}

		logger := a.logger.WithField("notification", notification)
		if err := queue(ctx, notification); err != nil {
			logger.WithError(err).Error("failed to queue notification")
			return err
		}

		a.logger.Debug("send catchup")
	}

	return rows.Close()
}

func (a *AggregateProjector) markProjectionAsFailed(ctx context.Context, notification *projector.Notification) error {
	conn, err := internal.AcquireConn(ctx, a.db)
	if err != nil {
		return err
	}

	defer func() {
		if err := conn.Close(); err != nil {
			a.logger.WithError(err).Warn("failed to db close failure connection")
		}
	}()

	return a.storage.PersistFailure(ctx, conn, notification)
}
