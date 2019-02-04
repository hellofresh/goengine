package inmemory

import (
	"errors"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/internal/reflect"
)

var (
	// ErrUnknownPayloadType occurs when a payload type is unknown
	ErrUnknownPayloadType = errors.New("unknown payload type was provided")
	// ErrDuplicatePayloadType occurs when a payload type is already registered
	ErrDuplicatePayloadType = errors.New("payload type is already registered")
	// Ensure that we satisfy the eventstore.MessagePayloadResolver interface
	_ eventstore.MessagePayloadResolver = &PayloadRegistry{}
)

// PayloadRegistry is a registry containing the mapping of an payload type to a event name
type PayloadRegistry struct {
	typeMap map[string]string
}

// RegisterPayload register a eventName to a specific payload type.
// Reflection is used to determine the full payload type name.
func (p *PayloadRegistry) RegisterPayload(eventName string, payload interface{}) error {
	name := reflect.FullTypeNameOf(payload)
	if _, found := p.typeMap[name]; found {
		return ErrDuplicatePayloadType
	}

	if p.typeMap == nil {
		p.typeMap = map[string]string{
			name: eventName,
		}
	} else {
		p.typeMap[name] = eventName
	}

	return nil
}

// ResolveName resolves the type name based on the underlying type of the payload
func (p *PayloadRegistry) ResolveName(payload interface{}) (string, error) {
	name := reflect.FullTypeNameOf(payload)
	if eventName, found := p.typeMap[name]; found {
		return eventName, nil
	}

	return "", ErrUnknownPayloadType
}
