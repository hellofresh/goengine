// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/hellofresh/goengine/v2/driver/sql (interfaces: ProjectionStateSerialization)

// Package sql is a generated GoMock package.
package sql

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// ProjectionStateSerialization is a mock of ProjectionStateSerialization interface.
type ProjectionStateSerialization struct {
	ctrl     *gomock.Controller
	recorder *ProjectionStateSerializationMockRecorder
}

// ProjectionStateSerializationMockRecorder is the mock recorder for ProjectionStateSerialization.
type ProjectionStateSerializationMockRecorder struct {
	mock *ProjectionStateSerialization
}

// NewProjectionStateSerialization creates a new mock instance.
func NewProjectionStateSerialization(ctrl *gomock.Controller) *ProjectionStateSerialization {
	mock := &ProjectionStateSerialization{ctrl: ctrl}
	mock.recorder = &ProjectionStateSerializationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *ProjectionStateSerialization) EXPECT() *ProjectionStateSerializationMockRecorder {
	return m.recorder
}

// DecodeState mocks base method.
func (m *ProjectionStateSerialization) DecodeState(arg0 []byte) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DecodeState", arg0)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DecodeState indicates an expected call of DecodeState.
func (mr *ProjectionStateSerializationMockRecorder) DecodeState(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DecodeState", reflect.TypeOf((*ProjectionStateSerialization)(nil).DecodeState), arg0)
}

// EncodeState mocks base method.
func (m *ProjectionStateSerialization) EncodeState(arg0 interface{}) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EncodeState", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// EncodeState indicates an expected call of EncodeState.
func (mr *ProjectionStateSerializationMockRecorder) EncodeState(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EncodeState", reflect.TypeOf((*ProjectionStateSerialization)(nil).EncodeState), arg0)
}

// Init mocks base method.
func (m *ProjectionStateSerialization) Init(arg0 context.Context) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Init indicates an expected call of Init.
func (mr *ProjectionStateSerializationMockRecorder) Init(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*ProjectionStateSerialization)(nil).Init), arg0)
}
