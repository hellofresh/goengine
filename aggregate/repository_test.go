package aggregate_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewRepository(t *testing.T) {
	eventStore := &mocks.EventStore{}
	aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
		return &mocks.AggregateRoot{}
	})

	t.Run("create a new repository", func(t *testing.T) {
		repo, err := aggregate.NewRepository(
			eventStore,
			"event_stream",
			aggregateType,
		)

		asserts := assert.New(t)
		asserts.NotNil(repo, "Expected a repository to be returned")
		asserts.Nil(err, "Expected no error")
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title         string
			eventStore    eventstore.EventStore
			streamName    eventstore.StreamName
			aggregateType *aggregate.Type
			expectedError error
		}

		testCases := []invalidTestCase{
			{
				"requires a event store",
				nil,
				"event_stream",
				aggregateType,
				aggregate.ErrEventStoreRequired,
			},
			{
				"requires a stream name",
				eventStore,
				"",
				aggregateType,
				aggregate.ErrStreamNameRequired,
			},
			{
				"requires a aggregate type",
				eventStore,
				"event_stream",
				nil,
				aggregate.ErrTypeRequired,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				repo, err := aggregate.NewRepository(
					testCase.eventStore,
					testCase.streamName,
					testCase.aggregateType,
				)

				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err)
				asserts.Nil(repo, "Expected no repository to be returned")
			})
		}
	})
}

func TestRepository_SaveAggregateRoot(t *testing.T) {
	t.Run("store any pending events", func(t *testing.T) {
		ctx := context.Background()

		rootID := aggregate.GenerateID()

		root := &mocks.AggregateRoot{}
		root.On("AggregateID").Return(rootID)
		root.On("Apply", mock.AnythingOfType("*aggregate.Changed"))

		repo, store := mockRepository()
		store.On("AppendTo", ctx, mock.Anything, mock.Anything).Return(nil)

		// Record the events
		events := []struct{ order int }{
			{order: 1},
			{order: 2},
		}
		for _, event := range events {
			aggregate.RecordChange(root, event)
		}
		err := repo.SaveAggregateRoot(context.Background(), root)

		asserts := assert.New(t)
		asserts.Nil(err, "Expect no error")
		store.AssertExpectations(t)

		calls := mocks.FetchFuncCalls(store.Calls, "AppendTo")
		if !asserts.Len(calls, 1) {
			return
		}

		call := calls[0]
		if !asserts.IsType(
			([]messaging.Message)(nil),
			call.Arguments[2],
			"Expected AppendTo to be called with the pending Changes",
		) {
			return
		}

		messages := call.Arguments[2].([]messaging.Message)
		for i, msg := range messages {
			msgMeta := msg.Metadata()

			asserts.Equalf(events[i], msg.Payload(), "Expect payload to equal event %d", i)
			asserts.Equal(rootID, msgMeta.Value(aggregate.IDKey))
			asserts.Equal("mock", msgMeta.Value(aggregate.TypeKey))
			asserts.Equal(uint(i+1), msgMeta.Value(aggregate.VersionKey))
		}
	})

	t.Run("store nothing when there are no pending events", func(t *testing.T) {
		ctx := context.Background()

		repo, store := mockRepository()
		store.On("AppendTo", ctx, mock.Anything, mock.Anything).Return(nil)

		err := repo.SaveAggregateRoot(ctx, &mocks.AggregateRoot{})

		assert.Nil(t, err, "Expect no error")
		store.AssertNotCalled(t, "AppendTo", ctx, mock.Anything, mock.Anything)
	})

	t.Run("reject aggregates of a different type", func(t *testing.T) {
		repo, _ := mockRepository()

		err := repo.SaveAggregateRoot(context.Background(), &mocks.AnotherAggregateRoot{})

		assert.Equal(t, aggregate.ErrUnsupportedAggregateType, err)
	})
}

func TestRepository_GetAggregateRoot(t *testing.T) {
	t.Run("load and reconstitute a AggregateRoot", func(t *testing.T) {
		asserts := assert.New(t)
		ctx := context.Background()
		rootID := aggregate.GenerateID()

		firstEvent, _ := aggregate.ReconstituteChange(
			rootID,
			messaging.GenerateUUID(),
			struct{ order int }{order: 1},
			nil,
			time.Now().UTC(),
			1,
		)
		secondEvent, _ := aggregate.ReconstituteChange(
			rootID,
			messaging.GenerateUUID(),
			struct{ order int }{order: 2},
			nil,
			time.Now().UTC(),
			2,
		)
		eventStream := []*aggregate.Changed{firstEvent, secondEvent}
		messageStream, err := inmemory.NewEventStream([]messaging.Message{firstEvent, secondEvent}, []int64{1, 2})
		if !asserts.NoError(err) {
			return
		}

		store := &mocks.EventStore{}
		aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
			root := &mocks.AggregateRoot{}
			root.On("Apply", mock.AnythingOfType("*aggregate.Changed"))

			return root
		})

		store.
			On(
				"Load",
				ctx,
				eventstore.StreamName("event_stream"),
				1,
				(*uint)(nil),
				mock.MatchedBy(func(m metadata.Matcher) bool {
					expected := metadata.WithConstraint(
						metadata.WithConstraint(
							metadata.NewMatcher(),
							aggregate.TypeKey,
							metadata.Equals,
							"mock",
						),
						aggregate.IDKey,
						metadata.Equals,
						rootID,
					)

					return assert.Equal(t, expected, m)
				}),
			).
			Return(messageStream, nil).
			Once()

		repo, _ := aggregate.NewRepository(
			store,
			"event_stream",
			aggregateType,
		)

		// Get/Load the aggregate
		root, err := repo.GetAggregateRoot(ctx, rootID)

		if !asserts.NoError(err) {
			asserts.FailNow("Expected no error")
		}

		asserts.NotNil(root, "Expected a aggregate root")
		store.AssertExpectations(t)

		rootAggregate := root.(*mocks.AggregateRoot)
		rootAggregate.AssertExpectations(t)

		appendCalls := mocks.FetchFuncCalls(rootAggregate.Calls, "Apply")
		asserts.Len(appendCalls, 2)
		for i, call := range appendCalls {
			asserts.Equal(eventStream[i], call.Arguments[0])
		}
	})

	t.Run("load failures", func(t *testing.T) {
		type badEventStore struct {
			title         string
			storeMessages []messaging.Message
			storeError    error
			expectedError error
		}

		badTestCases := []badEventStore{
			{
				"fail to load when the event store returns an error",
				nil,
				errors.New("ERROR"),
				errors.New("ERROR"),
			},
			{
				"fail to load when the eventstore returns unexpected message types",
				[]messaging.Message{nil},
				nil,
				aggregate.ErrUnexpectedMessageType,
			},
			{
				"fail to load when the eventstore returns empty events",
				[]messaging.Message{},
				nil,
				aggregate.ErrEmptyEventStream,
			},
		}

		for _, testCase := range badTestCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)
				ctx := context.Background()
				rootID := aggregate.GenerateID()

				stream, err := inmemory.NewEventStream(testCase.storeMessages, make([]int64, len(testCase.storeMessages)))
				if !asserts.NoError(err) {
					return
				}

				repo, store := mockRepository()
				store.
					On(
						"Load",
						ctx,
						eventstore.StreamName("event_stream"),
						1,
						(*uint)(nil),
						mock.Anything,
					).
					Return(stream, testCase.storeError).
					Once()

				// Get/Load the aggregate
				root, err := repo.GetAggregateRoot(ctx, rootID)

				asserts.Equal(testCase.expectedError, err, "Expected error")
				asserts.Nil(root, "Expected no aggregate root")
				store.AssertExpectations(t)
			})
		}
	})
}

func mockRepository() (*aggregate.Repository, *mocks.EventStore) {
	eventStore := &mocks.EventStore{}
	aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
		return &mocks.AggregateRoot{}
	})

	repo, _ := aggregate.NewRepository(
		eventStore,
		"event_stream",
		aggregateType,
	)

	return repo, eventStore
}
