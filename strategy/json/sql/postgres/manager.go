package postgres

import (
	"database/sql"
	"time"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/strategy/json"
	strategySQL "github.com/hellofresh/goengine/strategy/json/sql"
)

// SingleStreamManager is a helper for creating JSON Postgres event stores and projectors
type SingleStreamManager struct {
	db                  *sql.DB
	payloadTransformer  *json.PayloadTransformer
	persistenceStrategy driverSQL.PersistenceStrategy
	messageFactory      driverSQL.MessageFactory

	logger  goengine.Logger
	metrics driverSQL.Metrics
}

// NewSingleStreamManager return a new instance of the SingleStreamManager
func NewSingleStreamManager(db *sql.DB, logger goengine.Logger, metrics driverSQL.Metrics) (*SingleStreamManager, error) {
	if db == nil {
		return nil, goengine.InvalidArgumentError("rdb")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}
	if metrics == nil {
		metrics = driverSQL.NopMetrics
	}

	payloadTransformer := json.NewPayloadTransformer()

	// Setting up the postgres strategy
	persistenceStrategy, err := NewSingleStreamStrategy(payloadTransformer)
	if err != nil {
		return nil, err
	}

	// Setting up the message factory
	messageFactory, err := strategySQL.NewAggregateChangedFactory(payloadTransformer)
	if err != nil {
		return nil, err
	}

	return &SingleStreamManager{
		db:                  db,
		payloadTransformer:  payloadTransformer,
		persistenceStrategy: persistenceStrategy,
		messageFactory:      messageFactory,
		logger:              logger,
		metrics:             metrics,
	}, nil
}

// NewEventStore returns a new event store instance
func (m *SingleStreamManager) NewEventStore() (*postgres.EventStore, error) {
	// Setting up the event store
	return postgres.NewEventStore(
		m.persistenceStrategy,
		m.db,
		m.messageFactory,
		m.logger,
	)
}

// RegisterPayloads registers a set of payload type initiators
func (m *SingleStreamManager) RegisterPayloads(initiators map[string]json.PayloadInitiator) error {
	return m.payloadTransformer.RegisterPayloads(initiators)
}

// PersistenceStrategy returns the sql persistence strategy
func (m *SingleStreamManager) PersistenceStrategy() driverSQL.PersistenceStrategy {
	return m.persistenceStrategy
}

// NewStreamProjector returns a new stream projector instance
func (m *SingleStreamManager) NewStreamProjector(
	rdb *sql.DB,
	projectionTable string,
	projection goengine.Projection,
	projectionErrorHandler driverSQL.ProjectionErrorCallback,
	useLockedField bool,
) (*driverSQL.StreamProjector, error) {
	eventStore, err := m.NewEventStore()
	if err != nil {
		return nil, err
	}

	projectorStorage, err := postgres.NewAdvisoryLockStreamProjectionStorage(
		projection.Name(),
		projectionTable,
		driverSQL.GetProjectionStateSerialization(projection),
		useLockedField,
		m.logger,
	)
	if err != nil {
		return nil, err
	}

	return driverSQL.NewStreamProjector(
		rdb,
		m.db,
		driverSQL.StreamProjectionEventStreamLoader(eventStore, projection.FromStream()),
		m.payloadTransformer,
		projection,
		projectorStorage,
		projectionErrorHandler,
		m.logger,
	)
}

// NewAggregateProjector returns a new aggregate projector instance
func (m *SingleStreamManager) NewAggregateProjector(
	eventStream goengine.StreamName,
	aggregateTypeName string,
	projectionTable string,
	projection goengine.Projection,
	projectionErrorHandler driverSQL.ProjectionErrorCallback,
	useLockedField bool,
	retryDelay time.Duration,
) (*driverSQL.AggregateProjector, error) {
	eventStore, err := m.NewEventStore()
	if err != nil {
		return nil, err
	}

	eventStoreTable, err := m.persistenceStrategy.GenerateTableName(eventStream)
	if err != nil {
		return nil, err
	}

	projectorStorage, err := postgres.NewAdvisoryLockAggregateProjectionStorage(
		eventStoreTable,
		projectionTable,
		driverSQL.GetProjectionStateSerialization(projection),
		useLockedField,
		m.logger,
	)
	if err != nil {
		return nil, err
	}

	return driverSQL.NewAggregateProjector(
		m.db,
		driverSQL.AggregateProjectionEventStreamLoader(eventStore, projection.FromStream(), aggregateTypeName),
		m.payloadTransformer,
		projection,
		projectorStorage,
		projectionErrorHandler,
		m.logger,
		m.metrics,
		retryDelay,
	)
}
