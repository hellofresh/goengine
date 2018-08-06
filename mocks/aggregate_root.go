package mocks

import (
	"github.com/hellofresh/goengine/aggregate"
	"github.com/stretchr/testify/mock"
)

type (
	// AggregateRoot is an mock type for the AggregateRoot type
	AggregateRoot struct {
		mock.Mock
		aggregate.BaseRoot
	}

	// AnotherAggregateRoot is an mock type for the AggregateRoot type
	AnotherAggregateRoot struct {
		mock.Mock
		aggregate.BaseRoot
	}
)

// AggregateID provides a mock function
func (_m *AggregateRoot) AggregateID() aggregate.ID {
	ret := _m.Called()

	var r0 aggregate.ID
	if rf, ok := ret.Get(0).(func() aggregate.ID); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(aggregate.ID)
		}
	}

	return r0
}

// Apply provides a mock function with given fields: event
func (_m *AggregateRoot) Apply(event *aggregate.Changed) {
	_m.Called(event)
}

// AggregateID provides a mock function
func (_m *AnotherAggregateRoot) AggregateID() aggregate.ID {
	ret := _m.Called()

	var r0 aggregate.ID
	if rf, ok := ret.Get(0).(func() aggregate.ID); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(aggregate.ID)
		}
	}

	return r0
}

// Apply provides a mock function with given fields: event
func (_m *AnotherAggregateRoot) Apply(event *aggregate.Changed) {
	_m.Called(event)
}
