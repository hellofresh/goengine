// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/hellofresh/goengine/v2 (interfaces: EventStream)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	goengine "github.com/hellofresh/goengine/v2"
)

// EventStream is a mock of EventStream interface.
type EventStream struct {
	ctrl     *gomock.Controller
	recorder *EventStreamMockRecorder
}

// EventStreamMockRecorder is the mock recorder for EventStream.
type EventStreamMockRecorder struct {
	mock *EventStream
}

// NewEventStream creates a new mock instance.
func NewEventStream(ctrl *gomock.Controller) *EventStream {
	mock := &EventStream{ctrl: ctrl}
	mock.recorder = &EventStreamMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *EventStream) EXPECT() *EventStreamMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *EventStream) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *EventStreamMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*EventStream)(nil).Close))
}

// Err mocks base method.
func (m *EventStream) Err() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err.
func (mr *EventStreamMockRecorder) Err() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*EventStream)(nil).Err))
}

// Message mocks base method.
func (m *EventStream) Message() (goengine.Message, int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Message")
	ret0, _ := ret[0].(goengine.Message)
	ret1, _ := ret[1].(int64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Message indicates an expected call of Message.
func (mr *EventStreamMockRecorder) Message() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Message", reflect.TypeOf((*EventStream)(nil).Message))
}

// Next mocks base method.
func (m *EventStream) Next() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next.
func (mr *EventStreamMockRecorder) Next() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*EventStream)(nil).Next))
}
