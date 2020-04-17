// +build unit

package aggregate_test

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/hellofresh/goengine/aggregate"
	mocks "github.com/hellofresh/goengine/mocks/aggregate"
	"github.com/stretchr/testify/assert"
)

func TestGenerateID(t *testing.T) {
	asserts := assert.New(t)

	firstID := aggregate.GenerateID()
	asserts.NotEmpty(firstID, "A aggregate.ID should not be empty")

	secondID := aggregate.GenerateID()
	asserts.NotEmpty(secondID, "A aggregate.ID should not be empty")

	asserts.NotEqual(firstID, secondID, "Expected GenerateID() to return a different ID")
}

func TestRecordChange(t *testing.T) {
	t.Run("A change is recorded", func(t *testing.T) {
		asserts := assert.New(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rootID := aggregate.GenerateID()
		domainEvent := struct{}{}

		root := mocks.NewRoot(ctrl)
		root.EXPECT().AggregateID().Return(rootID)
		root.EXPECT().Apply(gomock.AssignableToTypeOf(&aggregate.Changed{})).Do(func(msg *aggregate.Changed) {
			asserts.Equal(rootID, msg.AggregateID())
			asserts.Equal(domainEvent, msg.Payload())
			asserts.Equal(uint(1), msg.Version())
		}).Times(1)

		// Record the change
		err := aggregate.RecordChange(root, domainEvent)

		// Check that the change was recorded
		asserts.NoError(err)
	})

	t.Run("Check required arguments", func(t *testing.T) {
		errorTestCases := []struct {
			title         string
			expectedError error
			aggregateID   aggregate.ID
			domainEvent   interface{}
		}{
			{
				title:         "aggregateID is required",
				expectedError: aggregate.ErrMissingAggregateID,
				aggregateID:   aggregate.ID(""),
				domainEvent:   struct{}{},
			},
			{
				title:         "message payload is required",
				expectedError: aggregate.ErrInvalidChangePayload,
				aggregateID:   aggregate.GenerateID(),
				domainEvent:   nil,
			},
		}

		for _, testCase := range errorTestCases {
			t.Run(testCase.title, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				root := mocks.NewRoot(ctrl)
				root.EXPECT().AggregateID().Return(testCase.aggregateID)

				// Record the change
				err := aggregate.RecordChange(root, testCase.domainEvent)

				// Check error
				assert.Equal(t, testCase.expectedError, err)
			})
		}
	})
}
