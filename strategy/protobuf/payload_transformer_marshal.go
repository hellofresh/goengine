package protobuf

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/hellofresh/goengine/strategy"
)

// ErrNotMarshalerPayload occurs when a PayloadInitiator is not implementing Marshaler interface
var ErrNotMarshalerPayload = errors.New("goengine: payload not implementing Marshaler interface")

// Ensure that MarshalPayloadTransformer satisfies the PayloadTransformer interface
var _ strategy.PayloadTransformer = &MarshalPayloadTransformer{}

// MarshalPayloadTransformer is a payload factory that can reconstruct payload from and to JSON
type MarshalPayloadTransformer struct {
	strategy.PayloadRegistryImplementation
}

// MarshablePayload ...
type MarshablePayload interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

// NewMarshalPayloadTransformer returns a new instance of the PayloadTransformer
func NewMarshalPayloadTransformer() *MarshalPayloadTransformer {
	return &MarshalPayloadTransformer{
		PayloadRegistryImplementation: *strategy.NewPayloadRegistryImplementation(),
	}
}

// ConvertPayload marshall the payload into JSON returning the payload fullpkgPath and the serialized data.
func (p *MarshalPayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

	payloadType, err := p.FindPayloadType(payloadName)
	if err != nil {
		return "", nil, err
	}

	if !payloadType.IsPointer() {
		val := reflect.ValueOf(payload)
		vp := reflect.New(val.Type())
		vp.Elem().Set(val)
		payload = vp.Interface()
	}

	m, ok := payload.(MarshablePayload)
	if !ok {
		return "", nil, ErrNotMarshalerPayload
	}

	data, err := m.Marshal()
	if err != nil {
		return "", nil, strategy.ErrPayloadCannotBeSerialized
	}

	return payloadName, data, nil
}

// CreatePayload reconstructs a payload based on it's type and the json data
func (p *MarshalPayloadTransformer) CreatePayload(typeName string, data interface{}) (interface{}, error) {
	var dataBytes []byte
	switch d := data.(type) {
	case []byte:
		dataBytes = d
	case string:
		dataBytes = bytes.NewBufferString(d).Bytes()
	default:
		return nil, strategy.ErrUnsupportedPayloadData
	}

	payloadType, err := p.FindPayloadType(typeName)
	if err != nil {
		return nil, err
	}

	if payloadType.IsPointer() {

		payload, ok := payloadType.Initiator().(MarshablePayload)
		if !ok {
			return nil, ErrNotMarshalerPayload
		}

		if err := payload.Unmarshal(dataBytes); err != nil {
			return nil, err
		}
		return payload, nil
	}

	val := reflect.ValueOf(payloadType.Initiator())
	vp := reflect.New(val.Type())
	vp.Elem().Set(val)
	payload, ok := vp.Interface().(MarshablePayload)
	if !ok {
		return nil, ErrNotMarshalerPayload
	}

	if err := payload.Unmarshal(dataBytes); err != nil {
		return nil, err
	}

	return vp.Elem().Interface(), nil
}
