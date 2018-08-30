package eventstore

import (
	"errors"
)

var (
	// ErrPayloadCannotBeSerialized occurs when the payload cannot be serialized
	ErrPayloadCannotBeSerialized = errors.New("payload cannot be serialized")
	// ErrPayloadNotRegistered occurs when the payload is not registered
	ErrPayloadNotRegistered = errors.New("payload is not registered")
	// ErrUnknownPayloadType occurs when a payload type is unknown
	ErrUnknownPayloadType = errors.New("unknown payload type provided")
	// ErrInitiatorInvalidResult occurs when a PayloadInitiator returns a reference to nil
	ErrInitiatorInvalidResult = errors.New("initializer must return a pointer that is not nil")
	// ErrDuplicatePayloadType occurs when a payload type is already registered
	ErrDuplicatePayloadType = errors.New("payload type is already registered")
)

type (
	// PayloadConverter an interface describing converting payload data
	PayloadConverter interface {
		// ConvertPayload generates unique name for the event_name
		ConvertPayload(payload interface{}) (name string, data []byte, err error)
	}

	// PayloadFactory is used to reconstruct message payloads
	PayloadFactory interface {
		// CreatePayload returns a reconstructed payload or a error
		CreatePayload(payloadType string, data interface{}) (interface{}, error)
	}

	// PayloadInitiator creates a new empty instance of a Payload
	// this instance can then be used to Unmarshal
	PayloadInitiator func() interface{}
)
