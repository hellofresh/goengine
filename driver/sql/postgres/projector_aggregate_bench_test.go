package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/strategy/json"
	strategySQL "github.com/hellofresh/goengine/strategy/json/sql"
	strategyPostgres "github.com/hellofresh/goengine/strategy/json/sql/postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	eventStream                     = goengine.StreamName("event_stream")
	_           goengine.Projection = &personProjection{}
)

const (
	personTypeName    = "person"
	personCreatedName = "personCreated"
	personUpdatedName = "personUpdated"
)

type (
	personCreated struct {
		idx int
	}
	personUpdated struct {
		idx int
	}

	personProjection struct {
	}
)

func (*personProjection) Init(ctx context.Context) (interface{}, error) {
	return nil, nil
}

func (*personProjection) Handlers() map[string]goengine.MessageHandler {
	return map[string]goengine.MessageHandler{
		personCreatedName: func(context.Context, interface{}, goengine.Message) (interface{}, error) {
			return nil, nil
		},
	}
}

func (*personProjection) Name() string {
	return "person_projection"
}

func (*personProjection) FromStream() goengine.StreamName {
	return eventStream
}

func BenchmarkAggregateProjector_Run(b *testing.B) {
	ctx := context.Background()
	projector, teardown := setup(
		b,
		func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (driverSQL.AggregateProjectorStorage, error) {
			return postgres.NewAdvisoryLockAggregateProjectionStorage(
				eventStoreTable,
				projectionTable,
				serialization,
				goengine.NopLogger,
			)
		},
	)
	defer teardown()

	b.ResetTimer()
	require.NoError(b, projector.Run(ctx))
}

func BenchmarkAggregateProjectorSkipLock_Run(b *testing.B) {
	ctx := context.Background()
	projector, teardown := setup(
		b,
		func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (driverSQL.AggregateProjectorStorage, error) {
			return postgres.NewSkipLockAggregateProjectionStorage(
				eventStoreTable,
				projectionTable,
				serialization,
				goengine.NopLogger,
			)
		},
	)
	defer teardown()

	b.ResetTimer()
	require.NoError(b, projector.Run(ctx))
}

func setup(
	b *testing.B,
	createStorage func(
		eventStoreTable,
		projectionTable string,
		projectionStateSerialization driverSQL.ProjectionStateSerialization,
		logger goengine.Logger,
	) (driverSQL.AggregateProjectorStorage, error),
) (*driverSQL.AggregateProjector, func()) {
	ctx := context.Background()
	projection := &personProjection{}

	// Fetch the postgres dsn from the env var
	dsn, exists := os.LookupEnv("POSTGRES_DSN")
	require.True(b, exists, "missing POSTGRES_DSN environment variable")

	db, err := sql.Open("postgres", dsn)
	require.NoError(b, err, "failed to open postgres driver")

	// Create payload transformer
	payloadTransformer := json.NewPayloadTransformer()
	require.NoError(b, err, payloadTransformer.RegisterPayload(personCreatedName, func() interface{} {
		return personCreated{}
	}))
	require.NoError(b, err, payloadTransformer.RegisterPayload(personUpdatedName, func() interface{} {
		return personUpdated{}
	}))

	// Use a persistence strategy
	persistenceStrategy, err := strategyPostgres.NewSingleStreamStrategy(payloadTransformer)
	require.NoError(b, err, "failed initializing persistent strategy")

	// Create message factory
	messageFactory, err := strategySQL.NewAggregateChangedFactory(payloadTransformer)
	require.NoError(b, err, "failed on dependencies load")

	// Create event store
	eventStore, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	require.NoError(b, err, "failed on dependencies load")

	// Setup the projection tables etc.
	eventStoreTable, err := persistenceStrategy.GenerateTableName(eventStream)
	projectionTable := "agg_projections"
	require.NoError(b, err, "failed to generate eventstream table name")

	projectorStorage, err := createStorage(
		eventStoreTable,
		projectionTable,
		driverSQL.NopProjectionStateSerialization{projection},
		goengine.NopLogger,
	)
	require.NoError(b, err, "failed to create projector storage")

	// Create the event stream
	err = eventStore.Create(ctx, eventStream)
	if err != postgres.ErrTableAlreadyExists {
		require.NoError(b, err, "failed on create event stream")
	}

	// Create projection tables
	queries := strategyPostgres.AggregateProjectorCreateSchema(projectionTable, eventStream, eventStoreTable)
	for _, query := range queries {
		_, err := db.ExecContext(ctx, query)
		require.NoError(b, err, "failed to create projection tables etc.")
	}

	projector, err := driverSQL.NewAggregateProjector(
		db,
		driverSQL.AggregateProjectionEventStreamLoader(eventStore, eventStream, personTypeName),
		payloadTransformer,
		projection,
		projectorStorage,
		func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
		goengine.NopLogger,
	)
	require.NoError(b, err, "failed to create aggregate projector")

	events := make([]goengine.Message, 0, b.N*2)
	for i := 0; i < b.N; i++ {
		aggID := aggregate.GenerateID()
		event, err := aggregate.ReconstituteChange(
			aggID,
			goengine.GenerateUUID(),
			personCreated{idx: i},
			metadata.WithValue(
				metadata.WithValue(
					metadata.WithValue(metadata.New(), aggregate.TypeKey, personTypeName),
					aggregate.VersionKey,
					float64(1),
				),
				aggregate.IDKey,
				string(aggID),
			),
			time.Now().UTC(),
			1,
		)
		require.NoError(b, err)
		events = append(events, event)

		event, err = aggregate.ReconstituteChange(
			aggID,
			goengine.GenerateUUID(),
			personUpdated{idx: i},
			metadata.WithValue(
				metadata.WithValue(
					metadata.WithValue(metadata.New(), aggregate.TypeKey, personTypeName),
					aggregate.VersionKey,
					float64(2),
				),
				aggregate.IDKey,
				string(aggID),
			),
			time.Now().UTC(),
			2,
		)
		require.NoError(b, err)
		events = append(events, event)
	}
	require.NoError(b, eventStore.AppendTo(ctx, eventStream, events), "failed to append events")

	return projector, func() {
		b.StopTimer()
		_, err = db.Exec("DROP TABLE events_event_stream")
		assert.NoError(b, err)

		_, err = db.Exec("DROP TABLE agg_projections")
		assert.NoError(b, err)

		_, err = db.Exec("DROP FUNCTION event_stream_notify()")
		assert.NoError(b, err)
	}
}
