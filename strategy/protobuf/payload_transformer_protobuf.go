package protobuf

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/hellofresh/goengine/strategy"
)

// ErrNotProtobufPayload occurs when a PayloadInitiator is not implementing proto.Message
var ErrNotProtobufPayload = errors.New("goengine: payload not implementing proto.Message")

// Ensure that PayloadTransformer satisfies the MessagePayloadConverter interface
var _ strategy.PayloadTransformer = &PayloadTransformer{}

// PayloadTransformer is a payload factory that can reconstruct payload from and to JSON
type PayloadTransformer struct {
	strategy.PayloadRegistryImplementation
}

// NewPayloadTransformer returns a new instance of the PayloadTransformer
func NewPayloadTransformer() *PayloadTransformer {
	return &PayloadTransformer{
		PayloadRegistryImplementation: *strategy.NewPayloadRegistryImplementation(),
	}
}

// ConvertPayload marshall the payload into Protobuf returning the payload fullpkgPath and the serialized data.
func (p *PayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

	payloadType, err := p.FindPayloadType(payloadName)
	if err != nil {
		return "", nil, err
	}

	if payloadType.IsPointer() {
		message, ok := payload.(proto.Message)
		if !ok {
			return "", nil, ErrNotProtobufPayload
		}
		data, err := proto.Marshal(message)
		if err != nil {
			return "", nil, strategy.ErrPayloadCannotBeSerialized
		}

		return payloadName, data, nil
	}

	vp := reflect.New(payloadType.ReflectionType()) // ptr to
	vp.Elem().Set(reflect.ValueOf(payload))
	message, ok := vp.Interface().(proto.Message)
	if !ok {
		return "", nil, ErrNotProtobufPayload
	}

	data, err := proto.Marshal(message)
	if err != nil {
		return "", nil, strategy.ErrPayloadCannotBeSerialized
	}

	return payloadName, data, nil
}

// CreatePayload reconstructs a payload based on it's type and the Protobuf data
func (p *PayloadTransformer) CreatePayload(typeName string, data interface{}) (interface{}, error) {
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
		payload, ok := payloadType.Initiator().(proto.Message)
		if !ok {
			return nil, ErrNotProtobufPayload
		}

		if err := proto.Unmarshal(dataBytes, payload); err != nil {
			return nil, err
		}

		return payload, nil
	}

	// Not a pointer so let's cry and use reflection
	payload := payloadType.Initiator()
	vp := reflect.New(payloadType.ReflectionType()) // ptr to
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
