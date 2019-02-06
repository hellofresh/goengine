// +build unit

package generic_test

import (
	"context"
	"testing"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/generic"
	"github.com/hellofresh/goengine/driver/inmemory"
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
		query.On("Handlers").Return(map[string]goengine.MessageHandler{
			"my_event": func(ctx context.Context, rawState interface{}, message goengine.Message) (interface{}, error) {
				return nil, nil
			},
		})

		executor, err := generic.NewQueryExecutor(store, "test", registry, query, 100)

		asserts := assert.New(t)
		asserts.NotNil(executor)
		asserts.NoError(err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title                 string
			eventStore            goengine.EventStore
			registry              goengine.MessagePayloadResolver
			streamName            goengine.StreamName
			query                 goengine.Query
			expectedArgumentError string
		}

		testCases := []invalidTestCase{
			{
				"eventStore may not be nil",
				nil,
				&mocks.PayloadResolver{},
				"event_stream",
				&mocks.Query{},
				"store",
			},
			{
				"resolver may not be nil",
				&inmemory.EventStore{},
				nil,
				"event_stream",
				&mocks.Query{},
				"resolver",
			},
			{
				"query may not be nil",
				&mocks.EventStore{},
				&mocks.PayloadResolver{},
				"event_stream",
				nil,
				"query",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				executor, err := generic.NewQueryExecutor(
					testCase.eventStore,
					testCase.streamName,
					testCase.registry,
					testCase.query,
					100,
				)

				asserts := assert.New(t)
				if asserts.Error(err) {
					arg := err.(goengine.InvalidArgumentError)
					asserts.Equal(testCase.expectedArgumentError, string(arg))
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
		eventBatch1, err := inmemory.NewEventStream(
			[]goengine.Message{
				mockMessageWithPayload(myEvent{1}, map[string]interface{}{}),
				mockMessageWithPayload(mySecondEvent{2}, map[string]interface{}{}),
			},
			[]int64{1, 2},
		)
		if !asserts.NoError(err) {
			return
		}

		eventBatch2, err := inmemory.NewEventStream(
			[]goengine.Message{
				mockMessageWithPayload(myEvent{3}, map[string]interface{}{}),
			},
			[]int64{3},
		)
		if !asserts.NoError(err) {
			return
		}

		var streamName goengine.StreamName = "event_stream"

		ctx := context.Background()

		queryBatchSize := uint(2)
		store := &mocks.EventStore{}
		store.On("Load", ctx, streamName, int64(1), &queryBatchSize, metadata.NewMatcher()).
			Once().
			Return(eventBatch1, nil)
		store.On("Load", ctx, streamName, int64(2), &queryBatchSize, metadata.NewMatcher()).
			Once().
			Return(eventBatch2, nil)

		registry := &mocks.PayloadResolver{}
		registry.On("ResolveName", mock.AnythingOfType("myEvent")).Return("my_event", nil)
		registry.On("ResolveName", mock.AnythingOfType("mySecondEvent")).Return("second_event", nil)

		query := &mocks.Query{}
		query.On("Init", ctx).Once().Return(myState{}, nil)
		query.On("Handlers").Times(2).Return(map[string]goengine.MessageHandler{
			"my_event": func(ctx context.Context, rawState interface{}, message goengine.Message) (interface{}, error) {
				state := rawState.(myState)
				state.count++
				state.numbers = append(
					state.numbers,
					message.Payload().(myEvent).number,
				)

				return state, nil
			},
			"second_event": func(ctx context.Context, rawState interface{}, message goengine.Message) (interface{}, error) {
				state := rawState.(myState)
				state.count++
				state.numbers = append(
					state.numbers,
					message.Payload().(mySecondEvent).number,
				)

				return state, nil
			},
		})

		executor, err := generic.NewQueryExecutor(store, streamName, registry, query, queryBatchSize)
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
