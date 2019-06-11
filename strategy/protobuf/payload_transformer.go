package protobuf

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/hellofresh/goengine"
	reflectUtil "github.com/hellofresh/goengine/internal/reflect"
)

var (
	// ErrUnsupportedProtobufPayloadData occurs when the data type is not supported by the PayloadTransformer
	ErrUnsupportedProtobufPayloadData = errors.New("goengine: payload data was expected to be a []byte or string")
	// ErrPayloadCannotBeSerialized occurs when the payload cannot be serialized
	ErrPayloadCannotBeSerialized = errors.New("goengine: payload cannot be serialized")
	// ErrPayloadNotRegistered occurs when the payload is not registered
	ErrPayloadNotRegistered = errors.New("goengine: payload is not registered")
	// ErrUnknownPayloadType occurs when a payload type is unknown
	ErrUnknownPayloadType = errors.New("goengine: unknown payload type provided")
	// ErrNotProtobufPayload occurs when a PayloadInitiator is not implementing proto.Message
	ErrNotProtobufPayload = errors.New("goengine: payload not implementing proto.Message")
	// ErrInitiatorInvalidResult occurs when a PayloadInitiator returns a reference to nil
	ErrInitiatorInvalidResult = errors.New("goengine: initializer must return a pointer that is not nil")
	// ErrDuplicatePayloadType occurs when a payload type is already registered
	ErrDuplicatePayloadType = errors.New("goengine: payload type is already registered")

	// Ensure that PayloadTransformer satisfies the MessagePayloadFactory interface
	_ goengine.MessagePayloadFactory = &PayloadTransformer{}
	// Ensure that PayloadTransformer satisfies the MessagePayloadConverter interface
	_ goengine.MessagePayloadConverter = &PayloadTransformer{}
	// Ensure that PayloadTransformer satisfies the MessagePayloadResolver interface
	_ goengine.MessagePayloadResolver = &PayloadTransformer{}
)

type (
	// PayloadInitiator creates a new empty instance of a Payload
	// this instance can then be used to Unmarshal
	PayloadInitiator func() interface{}

	// PayloadTransformer is a payload factory that can reconstruct payload from and to Protobuf
	PayloadTransformer struct {
		types map[string]PayloadType
		names map[string]string
	}

	// PayloadType represents a payload and the way to create it
	PayloadType struct {
		initiator      PayloadInitiator
		isPtr          bool
		reflectionType reflect.Type
	}
)

// NewPayloadTransformer returns a new instance of the PayloadTransformer
func NewPayloadTransformer() *PayloadTransformer {
	return &PayloadTransformer{
		types: map[string]PayloadType{},
		names: map[string]string{},
	}
}

// ConvertPayload marshall the payload into Protobuf returning the payload fullpkgPath and the serialized data.
func (p *PayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

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

// ResolveName returns the payloadType name of the provided payload
func (p *PayloadTransformer) ResolveName(payload interface{}) (string, error) {
	payloadName, ok := p.names[reflectUtil.FullTypeNameOf(payload)]
	if !ok {
		return "", ErrPayloadNotRegistered
	}

	return payloadName, nil
}

// RegisterPayload registers a payload type and the way to initialize it with the factory
func (p *PayloadTransformer) RegisterPayload(payloadType string, initiator PayloadInitiator) error {
	if _, known := p.types[payloadType]; known {
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

	p.names[reflectUtil.FullTypeName(rv.Type())] = payloadType

	p.types[payloadType] = PayloadType{
		initiator:      initiator,
		isPtr:          isPtr,
		reflectionType: rv.Type(),
	}

	return nil
}

// RegisterPayloads registers multiple payload types
func (p *PayloadTransformer) RegisterPayloads(payloads map[string]PayloadInitiator) error {
	for name, initiator := range payloads {
		if err := p.RegisterPayload(name, initiator); err != nil {
			return err
		}
	}

	return nil
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
		return nil, ErrUnsupportedProtobufPayloadData
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
