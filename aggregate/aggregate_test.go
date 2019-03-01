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

				assert.NoError(t, err)
				assert.Equal(t, testCase.output, id)
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
