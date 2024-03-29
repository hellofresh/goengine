// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/hellofresh/goengine/v2 (interfaces: MessagePayloadResolver)

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MessagePayloadResolver is a mock of MessagePayloadResolver interface.
type MessagePayloadResolver struct {
	ctrl     *gomock.Controller
	recorder *MessagePayloadResolverMockRecorder
}

// MessagePayloadResolverMockRecorder is the mock recorder for MessagePayloadResolver.
type MessagePayloadResolverMockRecorder struct {
	mock *MessagePayloadResolver
}

// NewMessagePayloadResolver creates a new mock instance.
func NewMessagePayloadResolver(ctrl *gomock.Controller) *MessagePayloadResolver {
	mock := &MessagePayloadResolver{ctrl: ctrl}
	mock.recorder = &MessagePayloadResolverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MessagePayloadResolver) EXPECT() *MessagePayloadResolverMockRecorder {
	return m.recorder
}

// ResolveName mocks base method.
func (m *MessagePayloadResolver) ResolveName(arg0 interface{}) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveName", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResolveName indicates an expected call of ResolveName.
func (mr *MessagePayloadResolverMockRecorder) ResolveName(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveName", reflect.TypeOf((*MessagePayloadResolver)(nil).ResolveName), arg0)
}
