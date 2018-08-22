package eventstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
)

var (
	// ErrUnsupportedJSONPayloadData occurs when the data type is not supported by the JSONPayloadFactory
	ErrUnsupportedJSONPayloadData = errors.New("payload data was expected to be a []byte, json.RawMessage or string")
	// ErrUnknownPayloadType occurs when a payload type is unknown
	ErrUnknownPayloadType = errors.New("unknown payload type provided")
	// ErrInitiatorInvalidResult occurs when a PayloadInitiator returns a reference to nil
	ErrInitiatorInvalidResult = errors.New("initializer must return a pointer that is not nil")
	// ErrDuplicatePayloadType occurs when a payload type is already registered
	ErrDuplicatePayloadType = errors.New("this payload type is already registered")
	// Ensure that JSONPayloadFactory satisfies the PayloadFactory interface
	_ PayloadFactory = &JSONPayloadFactory{}
)

type (
	// PayloadFactory is used to reconstruct message payloads
	PayloadFactory interface {
		// CreatePayload returns a reconstructed payload or a error
		CreatePayload(payloadType string, data interface{}) (interface{}, error)
	}

	// PayloadInitiator creates a new empty instance of a Payload
	// this instance can then be used to Unmarshal
	PayloadInitiator func() interface{}

	// JSONPayloadFactory is a payload factory that can reconstruct payload from JSON
	JSONPayloadFactory struct {
		types map[string]JSONPayloadType
	}

	// JSONPayloadType represents a payload and the way to create it
	JSONPayloadType struct {
		initiator      PayloadInitiator
		isPtr          bool
		reflectionType reflect.Type
	}
)

// NewJSONPayloadFactory returns a new instance of the JSONPayloadFactory
func NewJSONPayloadFactory() *JSONPayloadFactory {
	return &JSONPayloadFactory{
		types: map[string]JSONPayloadType{},
	}
}

// RegisterPayload registers a payload type and the way to initialize it with the factory
func (f *JSONPayloadFactory) RegisterPayload(payloadType string, initiator PayloadInitiator) error {
	if _, known := f.types[payloadType]; known {
		return ErrDuplicatePayloadType
	}

	checkPayload := initiator()
	if checkPayload == nil {
		return ErrInitiatorInvalidResult
	}

	rv := reflect.ValueOf(checkPayload)
	isPtr := rv.Kind() == reflect.Ptr
	if isPtr && rv.IsNil() {
		return ErrInitiatorInvalidResult
	}

	f.types[payloadType] = JSONPayloadType{
		initiator:      initiator,
		isPtr:          isPtr,
		reflectionType: rv.Type(),
	}

	return nil
}

// CreatePayload reconstructs a payload based on it's type and the json data
func (f *JSONPayloadFactory) CreatePayload(typeName string, data interface{}) (interface{}, error) {
	var dataBytes []byte
	switch d := data.(type) {
	case []byte:
		dataBytes = d
	case json.RawMessage:
		dataBytes = d
	case string:
		dataBytes = bytes.NewBufferString(d).Bytes()
	default:
		return nil, ErrUnsupportedJSONPayloadData
	}

	payloadType, found := f.types[typeName]
	if !found {
		return nil, ErrUnknownPayloadType
	}
	payload := payloadType.initiator()

	// Pointer we can handle nicely
	if payloadType.isPtr {
		if err := json.Unmarshal(dataBytes, payload); err != nil {
			return nil, err
		}
	}

	// Not a pointer so let's cry and use reflection
	vp := reflect.New(payloadType.reflectionType)
	vp.Elem().Set(reflect.ValueOf(payload))
	if err := json.Unmarshal(dataBytes, vp.Interface()); err != nil {
		return nil, err
	}

	return vp.Elem().Interface(), nil
}
