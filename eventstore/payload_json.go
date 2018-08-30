package eventstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
)

var (
	// ErrUnsupportedJSONPayloadData occurs when the data type is not supported by the JSONPayloadTransformer
	ErrUnsupportedJSONPayloadData = errors.New("payload data was expected to be a []byte, json.RawMessage or string")

	// Ensure that JSONPayloadTransformer satisfies the PayloadFactory interface
	_ PayloadFactory = &JSONPayloadTransformer{}
	// Ensure that JSONPayloadTransformer satisfies the PayloadConverter interface
	_ PayloadConverter = &JSONPayloadTransformer{}
)

type (
	// JSONPayloadTransformer is a payload factory that can reconstruct payload from and to JSON
	JSONPayloadTransformer struct {
		types map[string]JSONPayloadType
		names map[string]string
	}

	// JSONPayloadType represents a payload and the way to create it
	JSONPayloadType struct {
		initiator      PayloadInitiator
		isPtr          bool
		reflectionType reflect.Type
	}
)

// NewJSONPayloadTransformer returns a new instance of the JSONPayloadTransformer
func NewJSONPayloadTransformer() *JSONPayloadTransformer {
	return &JSONPayloadTransformer{
		types: map[string]JSONPayloadType{},
		names: map[string]string{},
	}
}

func (j *JSONPayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, ok := j.names[reflect.TypeOf(payload).String()]
	if !ok {
		return "", nil, ErrPayloadNotRegistered
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", nil, ErrPayloadCannotBeSerialized
	}

	return payloadName, data, nil
}

// RegisterPayload registers a payload type and the way to initialize it with the factory
func (j *JSONPayloadTransformer) RegisterPayload(payloadType string, initiator PayloadInitiator) error {
	if _, known := j.types[payloadType]; known {
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

	// This will store the fully qualified name of the event
	//
	// e.g: for a struct named OrderCreated located into file /events/events.go
	// this function will return events.OrderCreated
	j.names[rv.Type().String()] = payloadType

	j.types[payloadType] = JSONPayloadType{
		initiator:      initiator,
		isPtr:          isPtr,
		reflectionType: rv.Type(),
	}

	return nil
}

// CreatePayload reconstructs a payload based on it's type and the json data
func (j *JSONPayloadTransformer) CreatePayload(typeName string, data interface{}) (interface{}, error) {
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

	payloadType, found := j.types[typeName]
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
