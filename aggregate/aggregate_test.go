// +build unit

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

func TestIDFromString(t *testing.T) {
	t.Run("valid uuid", func(t *testing.T) {
		type uuidTestCase struct {
			title  string
			input  string
			output aggregate.ID
		}

		testCases := []uuidTestCase{
			{
				"UUID",
				"bf27f3c1-5fd6-4997-896a-63774ebd9ab0",
				aggregate.ID("bf27f3c1-5fd6-4997-896a-63774ebd9ab0"),
			},
			{
				"UUID with prefix",
				"urn:uuid:f4ec75db-c0b0-4b00-a04f-a0d9ed18e9fb",
				aggregate.ID("f4ec75db-c0b0-4b00-a04f-a0d9ed18e9fb"),
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				id, err := aggregate.IDFromString(testCase.input)

				asserts := assert.New(t)
				asserts.Nil(err)
				asserts.Equal(testCase.output, id)
			})
		}
	})

	t.Run("invalid uuid", func(t *testing.T) {
		type invalidTestCase struct {
			title string
			input string
		}

		testCases := []invalidTestCase{
			{
				"some string",
				"some string",
			},
			{
				"UUID with missing minuses",
				"f4ec75dbc0b04b00a04fa0d9ed18e9fb",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				id, err := aggregate.IDFromString(testCase.input)

				asserts := assert.New(t)
				asserts.Equal(aggregate.ErrInvalidID, err)
				asserts.Equal(aggregate.ID(""), id)
			})
		}
	})
}

func TestRecordChange(t *testing.T) {
	t.Run("A change is recorded", func(t *testing.T) {
		rootID := aggregate.GenerateID()
		domainEvent := struct{}{}

		root := &mocks.AggregateRoot{}
		root.On("AggregateID").Return(rootID)
		root.On("Apply", mock.AnythingOfType("*aggregate.Changed"))

		// Record the change
		err := aggregate.RecordChange(root, domainEvent)

		// Check that the change was recorded
		asserts := assert.New(t)
		asserts.Empty(err, "No error should be returned")

		root.AssertExpectations(t)
		calls := mocks.FetchFuncCalls(root.Calls, "Apply")
		if asserts.Len(calls, 1) {
			msg := calls[0].Arguments[0].(*aggregate.Changed)
			asserts.Equal(rootID, msg.AggregateID())
			asserts.Equal(domainEvent, msg.Payload())
			asserts.Equal(uint(1), msg.Version())
		}
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
				root := &mocks.AggregateRoot{}
				root.On("AggregateID").Return(testCase.aggregateID)
				root.On("Apply", mock.AnythingOfType("*aggregate.Changed"))

				// Record the change
				err := aggregate.RecordChange(root, testCase.domainEvent)

				// Check error
				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err)
				root.AssertNotCalled(t, "Apply", mock.Anything)
			})
		}
	})
}
