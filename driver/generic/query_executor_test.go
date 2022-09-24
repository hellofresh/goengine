//go:build unit

package generic_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/driver/generic"
	"github.com/hellofresh/goengine/v2/driver/inmemory"
	"github.com/hellofresh/goengine/v2/metadata"
	"github.com/hellofresh/goengine/v2/mocks"
)

func TestNewQueryExecutor(t *testing.T) {
	t.Run("Create Query Executor", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &mocks.EventStore{}
		registry := mocks.NewMessagePayloadResolver(ctrl)

		query := mocks.NewQuery(ctrl)
		query.EXPECT().Handlers().Return(map[string]goengine.MessageHandler{
			"my_event": func(ctx context.Context, rawState interface{}, message goengine.Message) (interface{}, error) {
				return nil, nil
			},
		}).Times(1)

		executor, err := generic.NewQueryExecutor(store, "test", registry, query, 100)

		assert.NotNil(t, executor)
		assert.NoError(t, err)
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
				&mocks.MessagePayloadResolver{},
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
				&mocks.MessagePayloadResolver{},
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
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedState := myState{
			count:   3,
			numbers: []int{1, 2, 3},
		}

		eventBatch1, err := inmemory.NewEventStream(
			[]goengine.Message{
				mockMessageWithPayload(myEvent{1}, map[string]interface{}{}),
				mockMessageWithPayload(mySecondEvent{2}, map[string]interface{}{}),
			},
			[]int64{1, 2},
		)
		require.NoError(t, err)

		eventBatch2, err := inmemory.NewEventStream(
			[]goengine.Message{
				mockMessageWithPayload(myEvent{3}, map[string]interface{}{}),
			},
			[]int64{3},
		)
		require.NoError(t, err)

		var streamName goengine.StreamName = "event_stream"

		ctx := context.Background()

		queryBatchSize := uint(2)
		store := mocks.NewEventStore(ctrl)
		store.EXPECT().Load(ctx, streamName, int64(1), &queryBatchSize, metadata.NewMatcher()).
			Return(eventBatch1, nil).Times(1)
		store.EXPECT().Load(ctx, streamName, int64(2), &queryBatchSize, metadata.NewMatcher()).
			Return(eventBatch2, nil).Times(1)

		registry := mocks.NewMessagePayloadResolver(ctrl)
		registry.EXPECT().ResolveName(gomock.AssignableToTypeOf(myEvent{})).Return("my_event", nil).AnyTimes()
		registry.EXPECT().ResolveName(gomock.AssignableToTypeOf(mySecondEvent{})).Return("second_event", nil).AnyTimes()

		query := mocks.NewQuery(ctrl)
		query.EXPECT().Init(ctx).Return(myState{}, nil).Times(1)
		query.EXPECT().Handlers().Return(map[string]goengine.MessageHandler{
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
		}).MinTimes(1)

		executor, err := generic.NewQueryExecutor(store, streamName, registry, query, queryBatchSize)
		require.NoError(t, err)

		finalState, err := executor.Run(ctx)

		assert.Equal(t, expectedState, finalState)
		assert.NoError(t, err)
	})
}

func mockMessageWithPayload(payload interface{}, metadataInfo map[string]interface{}) *mocks.DummyMessage {
	meta := metadata.New()
	for key, val := range metadataInfo {
		meta = metadata.WithValue(meta, key, val)
	}

	return mocks.NewDummyMessage(goengine.UUID{}, payload, metadata.FromMap(metadataInfo), time.Now())
}
