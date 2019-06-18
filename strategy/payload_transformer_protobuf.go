package strategy

import (
	"bytes"
	"reflect"

	"github.com/gogo/protobuf/proto"
)

// Ensure that ProtobufPayloadTransformer satisfies the MessagePayloadConverter interface
var _ PayloadTransformer = &ProtobufPayloadTransformer{}

// ProtobufPayloadTransformer is a payload factory that can reconstruct payload from and to JSON
type ProtobufPayloadTransformer struct {
	PayloadRegistryImplementation
}

// NewProtobufPayloadTransformer returns a new instance of the PayloadTransformer
func NewProtobufPayloadTransformer() *ProtobufPayloadTransformer {
	return &ProtobufPayloadTransformer{
		PayloadRegistryImplementation: PayloadRegistryImplementation{
			types: map[string]PayloadType{},
			names: map[string]string{},
		},
	}
}

// ConvertPayload marshall the payload into Protobuf returning the payload fullpkgPath and the serialized data.
func (p *ProtobufPayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

	payloadType, found := p.types[payloadName]
	if !found {
		return "", nil, ErrUnknownPayloadType
	}

	if payloadType.isPtr {
		message, ok := payload.(proto.Message)
		if !ok {
			return "", nil, ErrNotProtobufPayload
		}
		data, err := proto.Marshal(message)
		if err != nil {
			return "", nil, ErrPayloadCannotBeSerialized
		}

		return payloadName, data, nil
	}

	vp := reflect.New(payloadType.reflectionType) // ptr to
	vp.Elem().Set(reflect.ValueOf(payload))
	message, ok := vp.Interface().(proto.Message)
	if !ok {
		return "", nil, ErrNotProtobufPayload
	}

	data, err := proto.Marshal(message)
	if err != nil {
		return "", nil, ErrPayloadCannotBeSerialized
	}

	return payloadName, data, nil
}

// CreatePayload reconstructs a payload based on it's type and the Protobuf data
func (p *ProtobufPayloadTransformer) CreatePayload(typeName string, data interface{}) (interface{}, error) {
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
		payload, ok := payloadType.initiator().(proto.Message)
		if !ok {
			return nil, ErrNotProtobufPayload
		}

		if err := proto.Unmarshal(dataBytes, payload); err != nil {
			return nil, err
		}

		return payload, nil
	}

	// Not a pointer so let's cry and use reflection
	payload := payloadType.initiator()
	vp := reflect.New(payloadType.reflectionType) // ptr to
	vp.Elem().Set(reflect.ValueOf(payload))
	message, ok := vp.Interface().(proto.Message)
	if !ok {
		return nil, ErrNotProtobufPayload
	}

	if err := proto.Unmarshal(dataBytes, message); err != nil {
		return nil, err
	}

	return vp.Elem().Interface(), nil
}
