package aggregate_test

import (
	"testing"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestType(t *testing.T) {
	t.Parallel()

	mockInitiator := func() aggregate.Root {
		return &mocks.AggregateRoot{}
	}

	t.Run("Create a new type", func(t *testing.T) {
		t.Parallel()

		aggregateType, err := aggregate.NewType("mock", mockInitiator)

		asserts := assert.New(t)
		if !asserts.Empty(err, "No error should be returned") {
			return
		}
		asserts.NotEmpty(aggregateType, "A aggregate.Type should be returned")
		asserts.Equal("mock", aggregateType.String(), "Expected name to be set")

		t.Run("IsImplementedBy", func(t *testing.T) {
			asserts := assert.New(t)
			asserts.True(
				aggregateType.IsImplementedBy(&mocks.AggregateRoot{}),
				"Expected the same aggregate type to be valid",
			)
			asserts.False(
				aggregateType.IsImplementedBy(&mocks.AnotherAggregateRoot{}),
				"Expected another aggregate to not be of this type",
			)
			asserts.False(
				aggregateType.IsImplementedBy(mocks.AggregateRoot{}),
				"Expected a reference to an aggregate",
			)
			asserts.False(
				aggregateType.IsImplementedBy(nil),
				"Expected nil to not be of this type",
			)
			asserts.False(
				aggregateType.IsImplementedBy(&map[string]interface{}{}),
				"Expected anything that is not a struct reference to not be of this type",
			)
		})

		t.Run("CreateInstance", func(t *testing.T) {
			newInstance := aggregateType.CreateInstance()

			asserts := assert.New(t)
			asserts.NotNil(newInstance, "Expect the instance to not be empty")
			asserts.True(
				aggregateType.IsImplementedBy(newInstance),
				"Expected new instance of it's type",
			)
		})
	})

	t.Run("Check new required arguments", func(t *testing.T) {
		t.Parallel()

		t.Run("name is required", func(t *testing.T) {
			aggregateType, err := aggregate.NewType("", mockInitiator)

			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrTypeNameRequired, err, "Expect missing name error")
			asserts.Empty(aggregateType, "No type should be returned")
		})

		t.Run("initializer may not return nil", func(t *testing.T) {
			aggregateType, err := aggregate.NewType("init", func() aggregate.Root {
				return nil
			})

			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrInitiatorMustReturnRoot, err, "Expect invalid initializer")
			asserts.Empty(aggregateType, "No type should be returned")
		})

		t.Run("initializer may not return a nil pointer", func(t *testing.T) {
			aggregateType, err := aggregate.NewType("init", func() aggregate.Root {
				return (*mocks.AggregateRoot)(nil)
			})

			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrInitiatorMustReturnRoot, err, "Expect invalid initializer")
			asserts.Empty(aggregateType, "No type should be returned")
		})
	})
}
