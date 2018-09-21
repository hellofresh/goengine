// +build unit

package eventstore_test

import (
	"context"
	"testing"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewQueryExecutor(t *testing.T) {
	t.Run("Create Query Executor", func(t *testing.T) {
		store := &mocks.EventStore{}
		registry := &mocks.PayloadResolver{}
		query := &mocks.Query{}

		executor, err := eventstore.NewQueryExecutor(store, "test", registry, query)

		asserts := assert.New(t)
		asserts.NotNil(executor)
		asserts.NoError(err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title         string
			eventStore    eventstore.EventStore
			registry      eventstore.PayloadResolver
			streamName    eventstore.StreamName
			query         eventstore.Query
			expectedError error
		}

		testCases := []invalidTestCase{
			{
				"eventStore may not be nil",
				nil,
				&mocks.PayloadResolver{},
				"event_stream",
				&mocks.Query{},
				eventstore.ErrEventStoreRequired,
			},
			{
				"resolver may not be nil",
				&inmemory.EventStore{},
				nil,
				"event_stream",
				&mocks.Query{},
				eventstore.ErrPayloadResolverRequired,
			},
			{
				"query may not be nil",
				&mocks.EventStore{},
				&mocks.PayloadResolver{},
				"event_stream",
				nil,
				eventstore.ErrQueryRequired,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				executor, err := eventstore.NewQueryExecutor(
					testCase.eventStore,
					testCase.streamName,
					testCase.registry,
					testCase.query,
				)

				asserts := assert.New(t)
				if asserts.Error(err) {
					asserts.Equal(testCase.expectedError, err)
				}
				asserts.Nil(executor)
			})
		}
	})
}

func TestQueryExecutor_Run(t *testing.T) {
	type myState struct {
		count   int
		numbers []int
	}
	type myEvent struct {
		number int
	}
	type mySecondEvent struct {
		number int
	}

	t.Run("Run a Query", func(t *testing.T) {
		expectedState := myState{
			count:   3,
			numbers: []int{1, 2, 3},
		}

		asserts := assert.New(t)
		eventStream, err := inmemory.NewEventStream(
			[]messaging.Message{
				mockMessageWithPayload(myEvent{1}, map[string]interface{}{}),
				mockMessageWithPayload(mySecondEvent{2}, map[string]interface{}{}),
				mockMessageWithPayload(myEvent{3}, map[string]interface{}{}),
			},
			[]int64{1, 2, 3},
		)
		if !asserts.NoError(err) {
			return
		}

		var streamName eventstore.StreamName = "event_stream"

		ctx := context.Background()

		storeBatchSize := uint(100)
		store := &mocks.EventStore{}
		store.On("Load", ctx, streamName, int64(1), &storeBatchSize, metadata.NewMatcher()).
			Return(eventStream, nil)

		registry := &mocks.PayloadResolver{}
		registry.On("ResolveName", mock.AnythingOfType("myEvent")).Return("my_event", nil)
		registry.On("ResolveName", mock.AnythingOfType("mySecondEvent")).Return("second_event", nil)

		query := &mocks.Query{}
		query.On("Init").Once().Return(myState{})
		query.On("Handlers").Once().Return(map[string]eventstore.QueryMessageHandler{
			"my_event": func(rawState interface{}, message messaging.Message) (interface{}, error) {
				state := rawState.(myState)
				state.count++
				state.numbers = append(
					state.numbers,
					message.Payload().(myEvent).number,
				)

				return state, nil
			},
			"second_event": func(rawState interface{}, message messaging.Message) (interface{}, error) {
				state := rawState.(myState)
				state.count++
				state.numbers = append(
					state.numbers,
					message.Payload().(mySecondEvent).number,
				)

				return state, nil
			},
		})

		executor, err := eventstore.NewQueryExecutor(store, streamName, registry, query)
		if !asserts.NoError(err) {
			asserts.FailNow("failed to create executor")
		}

		finalState, err := executor.Run(ctx)

		asserts.Equal(expectedState, finalState)
		asserts.NoError(err)
	})
}

func mockMessageWithPayload(payload interface{}, metadataInfo map[string]interface{}) *mocks.Message {
	meta := metadata.New()
	for key, val := range metadataInfo {
		meta = metadata.WithValue(meta, key, val)
	}

	msg := &mocks.Message{}
	msg.On("Metadata").Return(meta)
	msg.On("Payload").Return(payload)

	return msg
}
