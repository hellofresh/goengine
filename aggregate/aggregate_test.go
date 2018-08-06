package aggregate_test

import (
	"testing"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	t.Parallel()

	t.Run("A change is recorded", func(t *testing.T) {
		rootID := aggregate.GenerateID()
		domainEvent := struct{}{}

		root := &mocks.AggregateRoot{}
		root.On("AggregateID").Return(rootID)
		root.On("Apply", mock.Anything).Once()

		// Record the change
		err := aggregate.RecordChange(root, domainEvent)

		// Check that the change was recorded
		asserts := assert.New(t)
		asserts.Empty(err, "No error should be returned")

		root.AssertExpectations(t)
		for _, call := range root.Calls {
			if call.Method != "Apply" {
				continue
			}

			asserts.IsType(
				(*aggregate.Changed)(nil),
				call.Arguments[0],
				"Expected Apply to be called with Changed reference",
			)

			msg := call.Arguments[0].(*aggregate.Changed)
			asserts.Equal(rootID, msg.AggregateID())
			asserts.Equal(domainEvent, msg.Payload())
			asserts.Equal(uint(1), msg.Version())
		}
	})

	t.Run("Check required arguments", func(t *testing.T) {
		t.Parallel()

		t.Run("aggregateID is required", func(t *testing.T) {
			root := &mocks.AggregateRoot{}
			root.On("AggregateID").Return(aggregate.ID(""))
			root.On("Apply", mock.Anything)

			domainEvent := struct{}{}

			// Record the change
			err := aggregate.RecordChange(root, domainEvent)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrMissingAggregateID, err)
			root.AssertNotCalled(t, "Apply", mock.Anything)
		})

		t.Run("message payload is required", func(t *testing.T) {
			root := &mocks.AggregateRoot{}
			root.On("AggregateID").Return(aggregate.GenerateID())
			root.On("Apply", mock.Anything)

			// Record the change
			err := aggregate.RecordChange(root, nil)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrInvalidChangePayload, err)
			root.AssertNotCalled(t, "Apply", mock.Anything)
		})
	})
}
