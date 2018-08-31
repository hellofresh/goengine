// +build unit

package inmemory_test

import (
	"context"
	"testing"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestNewQueryExecutor(t *testing.T) {
	t.Run("Create Query Executor", func(t *testing.T) {
		store := &inmemory.EventStore{}
		registry := &inmemory.PayloadRegistry{}
		query := &mocks.Query{}

		executor, err := inmemory.NewQueryExecutor(store, "test", registry, query)

		asserts := assert.New(t)
		asserts.NotNil(executor)
		asserts.NoError(err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title         string
			eventStore    *inmemory.EventStore
			registry      *inmemory.PayloadRegistry
			streamName    eventstore.StreamName
			query         eventstore.Query
			expectedError error
		}

		testCases := []invalidTestCase{
			{
				"eventStore may not be nil",
				nil,
				&inmemory.PayloadRegistry{},
				"event_stream",
				&mocks.Query{},
				inmemory.ErrEventStoreRequired,
			},
			{
				"registry may not be nil",
				&inmemory.EventStore{},
				nil,
				"event_stream",
				&mocks.Query{},
				inmemory.ErrPayloadRegistryRequired,
			},
			{
				"query may not be nil",
				&inmemory.EventStore{},
				&inmemory.PayloadRegistry{},
				"event_stream",
				nil,
				inmemory.ErrQueryRequired,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				executor, err := inmemory.NewQueryExecutor(
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
		messages := []messaging.Message{
			mockMessageWithPayload(myEvent{1}, map[string]interface{}{}),
			mockMessageWithPayload(mySecondEvent{2}, map[string]interface{}{}),
			mockMessageWithPayload(myEvent{3}, map[string]interface{}{}),
		}

		var streamName eventstore.StreamName = "event_stream"

		ctx := context.Background()
		logger, _ := test.NewNullLogger()

		store := inmemory.NewEventStore(logger)
		store.Create(ctx, streamName)
		store.AppendTo(ctx, streamName, messages)

		registry := &inmemory.PayloadRegistry{}
		registry.RegisterPayload("my_event", myEvent{})
		registry.RegisterPayload("second_event", mySecondEvent{})

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

		executor, err := inmemory.NewQueryExecutor(store, streamName, registry, query)
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
