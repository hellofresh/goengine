// +build integration

package test_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"

	"github.com/hellofresh/goengine/strategy"
	strategySQL "github.com/hellofresh/goengine/strategy/sql"
	strategyPostgres "github.com/hellofresh/goengine/strategy/sql/postgres"

	"github.com/hellofresh/goengine/test/internal"
	"github.com/stretchr/testify/suite"
)

type (
	EventStoreTestSuite struct {
		internal.PostgresSuite

		createEventStore func(s *EventStoreTestSuite) goengine.EventStore
		createPayload    func(s *EventStoreTestSuite, i int32) interface{}
		equalPayload     func(s *EventStoreTestSuite, a interface{}, b interface{})

		eventStore goengine.EventStore
	}

	payloadData struct {
		Name    string
		Balance int32
	}
)

func NewJSONEventStoreTestSuite() *EventStoreTestSuite {
	return &EventStoreTestSuite{
		createEventStore: func(s *EventStoreTestSuite) goengine.EventStore {
			transformer := strategy.NewJSONPayloadTransformer()
			s.Require().NoError(
				transformer.RegisterPayload("tests", func() interface{} { return &payloadData{} }),
			)

			persistenceStrategy, err := strategyPostgres.NewSingleStreamStrategy(transformer)
			s.Require().NoError(err, "failed initializing persistent strategy")

			messageFactory, err := strategySQL.NewAggregateChangedFactory(transformer)
			s.Require().NoError(err, "failed on dependencies load")

			eventStore, err := postgres.NewEventStore(persistenceStrategy, s.DB(), messageFactory, nil)
			s.Require().NoError(err, "failed on dependencies load")

			return eventStore
		},
		createPayload: func(s *EventStoreTestSuite, i int32) interface{} {
			return &payloadData{Name: "json", Balance: i * 11}
		},
		equalPayload: func(s *EventStoreTestSuite, a interface{}, b interface{}) {
			s.Equal(a, b)
		},
	}
}

func NewProtobufEventStoreTestSuite() *EventStoreTestSuite {
	return &EventStoreTestSuite{
		createEventStore: func(s *EventStoreTestSuite) goengine.EventStore {
			transformer := strategy.NewProtobufPayloadTransformer()
			s.Require().NoError(
				transformer.RegisterPayload("tests", func() interface{} { return &internal.Payload{} }),
			)

			persistenceStrategy, err := strategyPostgres.NewSingleStreamStrategy(transformer)
			s.Require().NoError(err, "failed initializing persistent strategy")

			messageFactory, err := strategySQL.NewAggregateChangedFactory(transformer)
			s.Require().NoError(err, "failed on dependencies load")

			eventStore, err := postgres.NewEventStore(persistenceStrategy, s.DB(), messageFactory, nil)
			s.Require().NoError(err, "failed on dependencies load")

			return eventStore
		},
		createPayload: func(s *EventStoreTestSuite, i int32) interface{} {
			t := &internal.Payload{Name: "protobuf", Balance: i * 11}
			return t
		},
		equalPayload: func(s *EventStoreTestSuite, a interface{}, b interface{}) {
			s.True(proto.Equal(a.(proto.Message), b.(proto.Message)))
		},
	}
}

func TestEventStoreSuite(t *testing.T) {
	suite.Run(t, NewJSONEventStoreTestSuite())
	suite.Run(t, NewProtobufEventStoreTestSuite())
}

func (s *EventStoreTestSuite) SetupTest() {
	s.PostgresSuite.SetupTest()

	s.eventStore = s.createEventStore(s)
}

func (s *EventStoreTestSuite) TearDownTest() {
	s.eventStore = nil
	s.PostgresSuite.TearDownTest()
}

func (s *EventStoreTestSuite) TestCreate() {
	ctx := context.Background()

	err := s.eventStore.Create(ctx, "orders")
	s.Require().NoError(err)

	s.True(s.DBTableExists("events_orders"))

	var indexesCount int
	err = s.DB().QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'events_orders';`,
	).Scan(&indexesCount)
	s.Require().NoError(err)
	s.Equal(4, indexesCount)
}

func (s *EventStoreTestSuite) TestHasStream() {
	ctx := context.Background()

	streamName := goengine.StreamName("orders")
	anotherStreamName := goengine.StreamName("orders2")

	exists := s.eventStore.HasStream(ctx, streamName)
	s.Assert().False(exists)

	err := s.eventStore.Create(ctx, streamName)
	s.Require().NoError(err)

	exists = s.eventStore.HasStream(ctx, streamName)
	s.True(exists)

	exists = s.eventStore.HasStream(ctx, anotherStreamName)
	s.False(exists)
}

func (s *EventStoreTestSuite) TestAppendTo() {
	agregateID := goengine.GenerateUUID()
	ctx := context.Background()
	streamName := goengine.StreamName("orders_my")

	err := s.eventStore.Create(ctx, streamName)
	s.Require().NoError(err)

	messages := s.generateAppendMessages([]goengine.UUID{agregateID})
	err = s.eventStore.AppendTo(ctx, streamName, messages)
	s.Require().NoError(err)

	var count int
	rows, err := s.DB().QueryContext(ctx, `SELECT event_id from events_orders_my order by no ASC`)
	s.Require().NoError(err)
	defer func() {
		s.Require().NoError(rows.Close())
	}()

	count = 0
	for rows.Next() {
		var eventID goengine.UUID
		err := rows.Scan(&eventID)
		if s.NoError(err) {
			s.Equal(messages[count].UUID(), eventID)
		}
		count++
	}
	s.Equal(len(messages), count)
}

func (s *EventStoreTestSuite) TestLoad() {
	aggregateIDFirst := goengine.GenerateUUID()
	aggregateIDSecond := goengine.GenerateUUID()
	messages := s.generateAppendMessages([]goengine.UUID{aggregateIDFirst, aggregateIDSecond})
	countPrepared := int64(len(messages))
	ctx := context.Background()
	streamName := goengine.StreamName("orders_load")

	testCases := []struct {
		title          string
		fromNumber     int64
		count          *uint
		matcher        func() metadata.Matcher
		messages       []goengine.Message
		messageNumbers []int64
	}{
		{
			"Get all events from the storage",
			0,
			nil,
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages,
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			"Get only two first events",
			countPrepared - 1,
			nil,
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages[(countPrepared - 2):countPrepared],
			[]int64{9, 10},
		},
		{
			"Get only 5 last elements",
			6,
			nil,
			func() metadata.Matcher { return metadata.NewMatcher() },
			messages[5:],
			[]int64{6, 7, 8, 9, 10},
		},
		{
			"Get messages for one aggregate id",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, aggregateIDFirst)
			},
			messages[0 : countPrepared/2],
			[]int64{1, 2, 3, 4, 5},
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
			[]int64{2},
		},
		{
			"Get all messages with version 1",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_aggregate_version", metadata.Equals, 1)
			},
			[]goengine.Message{messages[0], messages[5]},
			[]int64{1, 6},
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
			[]int64{2, 3, 4},
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
			[]int64{1, 2, 3},
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
			[]int64{4, 5},
		},
		{
			"Get messages for not existing aggregate id",
			0,
			nil,
			func() metadata.Matcher {
				matcher := metadata.NewMatcher()
				return metadata.WithConstraint(matcher, "_aggregate_id", metadata.Equals, goengine.GenerateUUID())
			},
			nil,
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
			[]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
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
			nil,
		},
	}

	// create table
	err := s.eventStore.Create(ctx, streamName)
	s.Require().NoError(err)

	// store messages
	err = s.eventStore.AppendTo(ctx, streamName, messages)
	s.Require().NoError(err)

	initialConnectionCount := s.DB().Stats().OpenConnections
	for _, testCase := range testCases {
		s.Run(testCase.title, func() {
			expectedMessageCount := len(testCase.messages)
			s.Require().Len(testCase.messageNumbers, expectedMessageCount, "invalid test case messages len must be equal to messageNumbers")

			// read events
			storeLoadInstance := s.createEventStore(s)
			results, err := storeLoadInstance.Load(ctx, streamName, testCase.fromNumber, testCase.count, testCase.matcher())
			s.Require().NoError(err)

			var i int
			for results.Next() {
				resultEvent, resultNumber, err := results.Message()
				if !s.NoError(err) ||
					!s.Truef(expectedMessageCount > i, "unexpected message received %d", i) {
					i++
					continue
				}

				expectedEvent := testCase.messages[i]

				s.equalPayload(s, expectedEvent.Payload(), resultEvent.Payload())
				s.Equal(expectedEvent.UUID(), resultEvent.UUID())
				s.Equal(expectedEvent.Metadata().Value("_aggregate_type"), resultEvent.Metadata().Value("_aggregate_type"))
				s.Equal(expectedEvent.Metadata().Value("_aggregate_id"), resultEvent.Metadata().Value("_aggregate_id"))
				s.Equal(expectedEvent.Metadata().Value("_float_val"), resultEvent.Metadata().Value("_float_val"))

				aggregateVersionExpected := resultEvent.Metadata().Value("_aggregate_version").(float64)
				s.Equal(aggregateVersionExpected, resultEvent.Metadata().Value("_aggregate_version"))
				s.Equal(len(expectedEvent.Metadata().AsMap()), len(resultEvent.Metadata().AsMap()))

				s.Equal(testCase.messageNumbers[i], resultNumber)

				i++
			}

			s.NoError(results.Err())
			s.Equal(len(testCase.messages), i, "expected to have received the right amount of messages")

			s.NoError(results.Close())
			s.Equal(initialConnectionCount, s.DB().Stats().OpenConnections, "expected no more open connection than before the test ran")
		})
	}
}

func (s *EventStoreTestSuite) generateAppendMessages(aggregateIDs []goengine.UUID) []goengine.Message {
	var messages []goengine.Message
	for _, aggregateID := range aggregateIDs {
		for i := int32(0); i < 5; i++ {
			id := goengine.GenerateUUID()
			createdAt := time.Now()
			boolVal := true
			if i > 2 {
				boolVal = false
			}
			meta := s.appendMeta(map[string]interface{}{
				"_aggregate_version":             i + 1,
				"_aggregate_version_less_then_4": boolVal,
				"_aggregate_type":                "basic",
				"_aggregate_id":                  aggregateID.String(),
				"_float_val":                     float64(i) + float64(3.12),
				"rand_int":                       rand.Intn(100),
				"rand_float_32":                  rand.Float32(),
				"';''; DROP DATABASE events_orders_load;": "ok",
			})
			payload := s.createPayload(s, i)
			messages = append(
				messages,
				mocks.NewDummyMessage(id, payload, meta, createdAt),
			)
		}
	}
	return messages
}

func (s *EventStoreTestSuite) appendMeta(metadataInfo map[string]interface{}) metadata.Metadata {
	meta := metadata.New()
	for key, val := range metadataInfo {
		meta = metadata.WithValue(meta, key, val)
	}
	return meta
}
