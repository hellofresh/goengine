package strategy

import (
	"errors"
	"reflect"

	"github.com/hellofresh/goengine"
	reflectUtil "github.com/hellofresh/goengine/internal/reflect"
)

var (
	// ErrUnsupportedPayloadData occurs when the data type is not supported by the PayloadTransformer
	ErrUnsupportedPayloadData = errors.New("goengine: unexpected payload data type")
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

	// Ensure that PayloadRegistry satisfies the MessagePayloadResolver interface
	_ goengine.MessagePayloadResolver = &PayloadRegistryImplementation{}
)

type (
	// PayloadInitiator creates a new empty instance of a Payload
	// this instance can then be used to Unmarshal
	PayloadInitiator func() interface{}

	// PayloadRegistry defines interface for a PayloadRegistry used by PayloadTransformers
	PayloadRegistry interface {
		goengine.MessagePayloadResolver
		RegisterPayload(payloadType string, initiator PayloadInitiator) error
		RegisterPayloads(payloads map[string]PayloadInitiator) error
	}

	// PayloadRegistryImplementation is a payload factory that can reconstruct payload from and to Protobuf
	PayloadRegistryImplementation struct {
		PayloadRegistry
		types map[string]PayloadType
		names map[string]string
	}

	// PayloadType represents a payload and the way to create it
	PayloadType struct {
		initiator      PayloadInitiator
		isPtr          bool
		reflectionType reflect.Type
	}

	// PayloadTransformer is used to create and convert payloads
	PayloadTransformer interface {
		PayloadRegistry
		goengine.MessagePayloadFactory
		goengine.MessagePayloadConverter
	}
)

// RegisterPayload registers a payload type and the way to initialize it with the factory
func (p *PayloadRegistryImplementation) RegisterPayload(payloadType string, initiator PayloadInitiator) error {
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
func (p *PayloadRegistryImplementation) RegisterPayloads(payloads map[string]PayloadInitiator) error {
	for name, initiator := range payloads {
		if err := p.RegisterPayload(name, initiator); err != nil {
			return err
		}
	}

	return nil
}

// ResolveName returns the payloadType name of the provided payload
func (p *PayloadRegistryImplementation) ResolveName(payload interface{}) (string, error) {
	payloadName, ok := p.names[reflectUtil.FullTypeNameOf(payload)]
	if !ok {
		return "", ErrPayloadNotRegistered
	}

	return payloadName, nil
}
