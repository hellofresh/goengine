//go:build unit
// +build unit

package aggregate_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/aggregate"
	"github.com/hellofresh/goengine/v2/driver/inmemory"
	"github.com/hellofresh/goengine/v2/metadata"
	"github.com/hellofresh/goengine/v2/mocks"
	aggregateMocks "github.com/hellofresh/goengine/v2/mocks/aggregate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRepository(t *testing.T) {
	aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
		return &aggregateMocks.Root{}
	})

	t.Run("create a new repository", func(t *testing.T) {
		repo, err := aggregate.NewRepository(
			&mocks.EventStore{},
			"event_stream",
			aggregateType,
		)

		assert.NotNil(t, repo, "Expected a repository to be returned")
		assert.NoError(t, err)
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title         string
			eventStore    goengine.EventStore
			streamName    goengine.StreamName
			aggregateType *aggregate.Type
			expectedError error
		}

		testCases := []invalidTestCase{
			{
				"requires a event store",
				nil,
				"event_stream",
				aggregateType,
				goengine.InvalidArgumentError("eventStore"),
			},
			{
				"requires a stream name",
				&mocks.EventStore{},
				"",
				aggregateType,
				goengine.InvalidArgumentError("streamName"),
			},
			{
				"requires a aggregate type",
				&mocks.EventStore{},
				"event_stream",
				nil,
				goengine.InvalidArgumentError("aggregateType"),
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
		asserts := assert.New(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rootID := aggregate.GenerateID()
		events := []struct{ order int }{
			{order: 1},
			{order: 2},
		}

		root := aggregateMocks.NewRoot(ctrl)
		root.EXPECT().AggregateID().Return(rootID).AnyTimes()
		root.EXPECT().Apply(gomock.AssignableToTypeOf(&aggregate.Changed{})).Times(2)

		repo, store := mockRepository(ctrl)
		store.EXPECT().AppendTo(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf([]goengine.Message{})).Return(nil).
			Do(func(_ context.Context, _ goengine.StreamName, streamEvents []goengine.Message) {
				for i, msg := range streamEvents {
					msgMeta := msg.Metadata()

					asserts.Equalf(events[i], msg.Payload(), "Expect payload to equal event %d", i)
					asserts.Equal(rootID, msgMeta.Value(aggregate.IDKey))
					asserts.Equal("mock", msgMeta.Value(aggregate.TypeKey))
					asserts.Equal(uint(i+1), msgMeta.Value(aggregate.VersionKey))
				}
			}).Times(1)

		// Record the events
		for _, event := range events {
			require.NoError(t,
				aggregate.RecordChange(root, event),
			)
		}
		err := repo.SaveAggregateRoot(context.Background(), root)

		asserts.NoError(err)
	})

	t.Run("store nothing when there are no pending events", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()

		repo, _ := mockRepository(ctrl)
		err := repo.SaveAggregateRoot(ctx, aggregateMocks.NewRoot(ctrl))

		assert.Nil(t, err, "Expect no error")
	})

	t.Run("reject aggregates of a different type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		repo, _ := mockRepository(ctrl)
		err := repo.SaveAggregateRoot(context.Background(), aggregateMocks.NewAnotherRoot(ctrl))

		assert.Equal(t, aggregate.ErrUnsupportedAggregateType, err)
	})
}

func TestRepository_GetAggregateRoot(t *testing.T) {
	t.Run("load and reconstitute a AggregateRoot", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.Background()
		rootID := aggregate.GenerateID()

		firstEvent, _ := aggregate.ReconstituteChange(
			rootID,
			goengine.GenerateUUID(),
			struct{ order int }{order: 1},
			nil,
			time.Now().UTC(),
			1,
		)
		secondEvent, _ := aggregate.ReconstituteChange(
			rootID,
			goengine.GenerateUUID(),
			struct{ order int }{order: 2},
			nil,
			time.Now().UTC(),
			2,
		)
		messageStream, err := inmemory.NewEventStream([]goengine.Message{firstEvent, secondEvent}, []int64{1, 2})
		require.NoError(t, err)

		store := mocks.NewEventStore(ctrl)
		aggregateTypeCalls := 0
		aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
			root := aggregateMocks.NewRoot(ctrl)

			switch aggregateTypeCalls {
			case 0:
			case 1:
				gomock.InOrder(
					root.EXPECT().Apply(firstEvent),
					root.EXPECT().Apply(secondEvent),
				)
			default:
				t.Error("unexpected type creation")
			}
			aggregateTypeCalls++
			return root
		})

		store.EXPECT().Load(
			ctx,
			goengine.StreamName("event_stream"),
			int64(1),
			(*uint)(nil),
			metadata.WithConstraint(
				metadata.WithConstraint(
					metadata.NewMatcher(),
					aggregate.TypeKey,
					metadata.Equals,
					"mock",
				),
				aggregate.IDKey,
				metadata.Equals,
				rootID,
			),
		).Return(messageStream, nil).Times(1)

		repo, _ := aggregate.NewRepository(
			store,
			"event_stream",
			aggregateType,
		)

		// Get/Load the aggregate
		root, err := repo.GetAggregateRoot(ctx, rootID)

		assert.NoError(t, err)
		assert.NotNil(t, root, "Expected a aggregate root")
	})

	t.Run("load failures", func(t *testing.T) {
		type badEventStore struct {
			title         string
			storeMessages []goengine.Message
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
				[]goengine.Message{nil},
				nil,
				aggregate.ErrUnexpectedMessageType,
			},
			{
				"fail to load when the eventstore returns empty events",
				[]goengine.Message{},
				nil,
				aggregate.ErrEmptyEventStream,
			},
		}

		for _, testCase := range badTestCases {
			t.Run(testCase.title, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				ctx := context.Background()
				rootID := aggregate.GenerateID()

				stream, err := inmemory.NewEventStream(testCase.storeMessages, make([]int64, len(testCase.storeMessages)))
				require.NoError(t, err)

				repo, store := mockRepository(ctrl)
				store.EXPECT().Load(
					ctx,
					goengine.StreamName("event_stream"),
					int64(1),
					(*uint)(nil),
					gomock.Any(),
				).Return(stream, testCase.storeError).Times(1)

				// Get/Load the aggregate
				root, err := repo.GetAggregateRoot(ctx, rootID)

				assert.Equal(t, testCase.expectedError, err, "Expected error")
				assert.Nil(t, root, "Expected no aggregate root")
			})
		}
	})
}

func mockRepository(ctrl *gomock.Controller) (*aggregate.Repository, *mocks.EventStore) {
	eventStore := mocks.NewEventStore(ctrl)
	aggregateType, _ := aggregate.NewType("mock", func() aggregate.Root {
		return aggregateMocks.NewRoot(ctrl)
	})

	repo, _ := aggregate.NewRepository(
		eventStore,
		"event_stream",
		aggregateType,
	)

	return repo, eventStore
}
