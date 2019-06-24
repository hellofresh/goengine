package strategy

import (
	"bytes"
	"reflect"
)

// Ensure that MarshalPayloadTransformer satisfies the PayloadTransformer interface
var _ PayloadTransformer = &MarshalPayloadTransformer{}

// MarshalPayloadTransformer is a payload factory that can reconstruct payload from and to JSON
type MarshalPayloadTransformer struct {
	PayloadRegistryImplementation
}

// MarshablePayload ...
type MarshablePayload interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

// NewMarshalPayloadTransformer returns a new instance of the PayloadTransformer
func NewMarshalPayloadTransformer() *MarshalPayloadTransformer {
	return &MarshalPayloadTransformer{
		PayloadRegistryImplementation: PayloadRegistryImplementation{
			types: map[string]PayloadType{},
			names: map[string]string{},
		},
	}
}

// ConvertPayload marshall the payload into JSON returning the payload fullpkgPath and the serialized data.
func (p *MarshalPayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

	payloadType, found := p.types[payloadName]
	if !found {
		return "", nil, ErrUnknownPayloadType
	}

	if !payloadType.isPtr {
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
		return "", nil, ErrPayloadCannotBeSerialized
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
		return nil, ErrUnsupportedPayloadData
	}

	payloadType, found := p.types[typeName]
	if !found {
		return nil, ErrUnknownPayloadType
	}

	if payloadType.isPtr {

		payload, ok := payloadType.initiator().(MarshablePayload)
		if !ok {
			return nil, ErrNotMarshalerPayload
		}

		if err := payload.Unmarshal(dataBytes); err != nil {
			return nil, err
		}
		return payload, nil
	}

	val := reflect.ValueOf(payloadType.initiator())
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
