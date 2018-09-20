// +build integration

package postgres_test

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/hellofresh/goengine/eventstore"
	eventstorejson "github.com/hellofresh/goengine/eventstore/json"
	"github.com/hellofresh/goengine/eventstore/postgres"
	eventstoresql "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/internal/test"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

type payloadData struct {
	Name    string
	Balance int
}

func TestEventStoreCreate(t *testing.T) {
	t.Run("Check create table with indexes", func(t *testing.T) {
		asserts := assert.New(t)
		ctx := context.Background()
		test.PostgresDatabase(t, func(db *sql.DB) {
			store := initEventStore(t, db)
			err := store.Create(ctx, "orders")
			asserts.NoError(err)

			var existsTable bool
			err = db.QueryRowContext(
				ctx,
				`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'events_orders')`,
			).Scan(&existsTable)
			asserts.NoError(err)
			asserts.True(existsTable)

			var indexesCount int
			err = db.QueryRowContext(
				ctx,
				`	select count(1) from pg_indexes where indexname in ('events_orders_unique_index___aggregate_type__aggregate_id__aggregate_version', 'events_orders_index__aggregate_type__aggregate_id');`,
			).Scan(&indexesCount)
			asserts.NoError(err)
			asserts.Equal(2, indexesCount)
		})
	})
}

func TestEventStoreHasStream(t *testing.T) {
	t.Run("Check if stream exists", func(t *testing.T) {
		asserts := assert.New(t)
		ctx := context.Background()
		streamName := eventstore.StreamName("orders")
		anotherStreamName := eventstore.StreamName("orders2")
		test.PostgresDatabase(t, func(db *sql.DB) {
			store := initEventStore(t, db)
			exists := store.HasStream(ctx, streamName)
			asserts.False(exists)

			store.Create(ctx, streamName)
			exists = store.HasStream(ctx, streamName)
			asserts.True(exists)

			exists = store.HasStream(ctx, anotherStreamName)
			asserts.False(exists)
		})
	})
}

func TestEventStoreAppendTo(t *testing.T) {
	asserts := assert.New(t)
	t.Run("Check insert into the DB", func(t *testing.T) {
		agregateID := messaging.GenerateUUID()
		ctx := context.Background()
		streamName := eventstore.StreamName("orders_my")

		test.PostgresDatabase(t, func(db *sql.DB) {
			store := initEventStore(t, db)
			err := store.Create(ctx, streamName)
			asserts.NoError(err)

			messages := generateAppendMessages([]messaging.UUID{agregateID})
			err = store.AppendTo(ctx, streamName, messages)
			asserts.NoError(err)

			var count int
			rows, err := db.QueryContext(ctx, `SELECT event_id from events_orders_my order by no ASC`)
			if asserts.NoError(err) {
				defer rows.Close()
			}
			count = 0
			for rows.Next() {
				var eventID messaging.UUID
				err := rows.Scan(&eventID)
				if asserts.NoError(err) {
					asserts.Equal(messages[count].UUID(), eventID)
				}
				count++
			}
			asserts.Equal(len(messages), count)
		})
	})
}

func TestEventStoreLoad(t *testing.T) {
	asserts := assert.New(t)
	aggregateIDFirst := messaging.GenerateUUID()
	aggregateIDSecond := messaging.GenerateUUID()
	messages := generateAppendMessages([]messaging.UUID{aggregateIDFirst, aggregateIDSecond})
	countPrepared := len(messages)
	ctx := context.Background()
	streamName := eventstore.StreamName("orders_load")

	testCases := []struct {
		title      string
		fromNumber int
		count      *uint
		matcher    func() metadata.Matcher
		messages   []messaging.Message
	}{
		{
			"Get all events from the storage",
			0,
			nil,
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages,
		},
		{
			"Get only two first events",
			countPrepared - 1,
			nil,
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages[(countPrepared - 2):countPrepared],
		},
		{
			"Get only 5 last elements",
			0,
			uintPointer(5),
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages[:5],
		},
		{
			"Get messages for one aggregate id",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[0 : countPrepared/2+1],
		},
		{
			"Get messages for one aggregate id and version",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				matcher = metadata.WithConstraint(matcher, "_aggregate_version", metadata.Equals, 2)
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[1:2],
		},
		{
			"Get messages for less and grater then the version",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				matcher = metadata.WithConstraint(matcher, "_aggregate_version", metadata.GreaterThanEquals, 2)
				matcher = metadata.WithConstraint(matcher, "_aggregate_version", metadata.LowerThanEquals, 4)
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[1:4],
		},
		{
			"Get messages for boolean equals true",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				matcher = metadata.WithConstraint(matcher, "_aggregate_version_less_then_4", metadata.Equals, true)
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[:3],
		},
		{
			"Get messages for boolean equals false",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				matcher = metadata.WithConstraint(matcher, "_aggregate_version_less_then_4", metadata.Equals, false)
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[3:5],
		},
		{
			"Get messages for not existing aggregate id",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, messaging.GenerateUUID())
			},
			nil,
		},
		{
			"Get messages for metadata field with SQL injection",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "';''; DROP DATABASE events_orders_load;", metadata.Equals, "ok")
			},
			messages,
		},
		{
			"Get no messages for match with not existing metadata field",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_my_field_does_not_exist", metadata.Equals, "true")
			},
			nil,
		},
	}

	test.PostgresDatabase(t, func(db *sql.DB) {
		store := initEventStore(t, db)
		// create table
		err := store.Create(ctx, streamName)
		asserts.NoError(err)

		// store messages
		err = store.AppendTo(ctx, streamName, messages)
		asserts.NoError(err)

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				// read events
				storeLoadInstance := initEventStore(t, db)
				results, err := storeLoadInstance.Load(ctx, streamName, testCase.fromNumber, testCase.count, testCase.matcher())
				asserts.NoError(err)
				expectedMessages := testCase.messages

				for i, resultEvent := range results {
					expectedEvent := expectedMessages[i]

					asserts.Equal(expectedEvent.Payload(), resultEvent.Payload())
					asserts.Equal(expectedEvent.UUID(), resultEvent.UUID())
					asserts.Equal(expectedEvent.Metadata().Value("_aggregate_type"), resultEvent.Metadata().Value("_aggregate_type"))
					asserts.Equal(expectedEvent.Metadata().Value("_aggregate_id"), resultEvent.Metadata().Value("_aggregate_id"))
					asserts.Equal(expectedEvent.Metadata().Value("_float_val"), resultEvent.Metadata().Value("_float_val"))

					aggregateVersionExpected := resultEvent.Metadata().Value("_aggregate_version").(float64)
					asserts.Equal(aggregateVersionExpected, resultEvent.Metadata().Value("_aggregate_version"))
					asserts.Equal(len(expectedEvent.Metadata().AsMap()), len(resultEvent.Metadata().AsMap()))
				}
			})
		}
	})
}

func generateAppendMessages(aggregateIDs []messaging.UUID) []messaging.Message {
	var messages []messaging.Message
	for _, aggregateID := range aggregateIDs {
		for i := 0; i < 5; i++ {
			id := messaging.GenerateUUID()
			createdAt := time.Now()
			boolVal := true
			if i > 2 {
				boolVal = false
			}
			meta := appendMeta(map[string]interface{}{
				"_aggregate_version":                      i + 1,
				"_aggregate_version_less_then_4":          boolVal,
				"_aggregate_type":                         "basic",
				"_aggregate_id":                           aggregateID.String(),
				"_float_val":                              float64(i) + float64(3.12),
				"rand_int":                                rand.Intn(100),
				"rand_float_32":                           rand.Float32(),
				"';''; DROP DATABASE events_orders_load;": "ok",
			})
			payload := &payloadData{Name: "alice", Balance: i * 11}
			message := mockAppendMessage(id, payload, meta, createdAt)
			messages = append(messages, message)
		}
	}
	return messages
}

func appendMeta(metadataInfo map[string]interface{}) metadata.Metadata {
	meta := metadata.New()
	for key, val := range metadataInfo {
		meta = metadata.WithValue(meta, key, val)
	}
	return meta
}

func mockAppendMessage(id messaging.UUID, payload interface{}, meta interface{}, time time.Time) *mocks.Message {
	m := &mocks.Message{}
	m.On("UUID").Return(id)
	m.On("Payload").Return(payload)
	m.On("Metadata").Return(meta)
	m.On("CreatedAt").Return(time)
	return m
}

func uintPointer(int int) *uint {
	var i = uint(int)
	return &i
}

func initEventStore(t *testing.T, db *sql.DB) eventstore.EventStore {
	transformer := eventstorejson.NewPayloadTransformer()
	transformer.RegisterPayload("tests", func() interface{} { return &payloadData{} })
	persistenceStrategy, err := postgres.NewPostgresStrategy(transformer)
	if err != nil {
		t.Fatalf("failed initializing persistent strategy %s", err)
	}

	messageFactory, err := eventstoresql.NewAggregateChangedFactory(transformer)
	if err != nil {
		t.Fatalf("failed on dependencies load %s", err)
	}

	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	if err != nil {
		t.Fatalf("failed on dependencies load %s", err)
	}

	return store
}
