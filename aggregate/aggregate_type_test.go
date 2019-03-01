// +build unit

package aggregate_test

import (
	"testing"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	mocks "github.com/hellofresh/goengine/mocks/aggregate"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	mockInitiator := func() aggregate.Root {
		return &mocks.Root{}
	}

	t.Run("Create a new type", func(t *testing.T) {
		aggregateType, err := aggregate.NewType("mock", mockInitiator)

		asserts := assert.New(t)
		if !asserts.NoError(err, "No error should be returned") {
			return
		}
		asserts.NotEmpty(aggregateType, "A aggregate.Type should be returned")
		asserts.Equal("mock", aggregateType.String(), "Expected name to be set")
		asserts.True(
			aggregateType.IsImplementedBy(&mocks.Root{}),
			"Expected the same aggregate type to be valid",
		)

		invalidImplementations := []struct {
			title string
			value interface{}
		}{
			{
				title: "another aggregate is not of this type",
				value: &mocks.AnotherRoot{},
			},
			{
				title: "not a reference is not of this type",
				value: mocks.Root{},
			},
			{
				title: "nil is not of this type",
				value: nil,
			},
			{
				title: "anything that is not a struct reference is not of this type",
				value: &map[string]interface{}{},
			},
		}

		for _, testCase := range invalidImplementations {
			t.Run(testCase.title, func(t *testing.T) {
				assert.False(t, aggregateType.IsImplementedBy(testCase.value))
			})
		}

		t.Run("CreateInstance", func(t *testing.T) {
			newInstance := aggregateType.CreateInstance()

			assert.NotNil(t, newInstance, "Expect the instance to not be empty")
			assert.True(
				t,
				aggregateType.IsImplementedBy(newInstance),
				"Expected new instance of it's type",
			)
		})
	})

	t.Run("Check new required arguments", func(t *testing.T) {
		errorTestCases := []struct {
			title         string
			expectedError error
			name          string
			initiator     aggregate.Initiator
		}{
			{
				title:         "name is required",
				expectedError: goengine.InvalidArgumentError("name"),
				name:          "",
				initiator:     mockInitiator,
			},
			{
				title:         "initializer may not return nil",
				expectedError: aggregate.ErrInitiatorMustReturnRoot,
				name:          "init",
				initiator: func() aggregate.Root {
					return nil
				},
			},
			{
				title:         "initializer may not return a nil pointer",
				expectedError: aggregate.ErrInitiatorMustReturnRoot,
				name:          "init",
				initiator: func() aggregate.Root {
					return (*mocks.Root)(nil)
				},
			},
		}

		for _, testCase := range errorTestCases {
			t.Run(testCase.title, func(t *testing.T) {
				aggregateType, err := aggregate.NewType(testCase.name, testCase.initiator)

				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err, "Expect error")
				asserts.Nil(aggregateType, "No type should be returned")
			})
		}
	})
}
