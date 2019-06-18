package strategy

import (
	"bytes"
	"encoding/json"
	"reflect"

	"github.com/hellofresh/goengine/strategy/internal"
)

// Ensure that JSONPayloadTransformer satisfies the PayloadTransformer interface
var _ PayloadTransformer = &JSONPayloadTransformer{}

// JSONPayloadTransformer is a payload factory that can reconstruct payload from and to JSON
type JSONPayloadTransformer struct {
	PayloadRegistryImplementation
}

// NewJSONPayloadTransformer returns a new instance of the PayloadTransformer
func NewJSONPayloadTransformer() *JSONPayloadTransformer {
	return &JSONPayloadTransformer{
		PayloadRegistryImplementation: PayloadRegistryImplementation{
			types: map[string]PayloadType{},
			names: map[string]string{},
		},
	}
}

// ConvertPayload marshall the payload into JSON returning the payload fullpkgPath and the serialized data.
func (p *JSONPayloadTransformer) ConvertPayload(payload interface{}) (string, []byte, error) {
	payloadName, err := p.ResolveName(payload)
	if err != nil {
		return "", nil, err
	}

	data, err := internal.MarshalJSON(payload)
	if err != nil {
		return "", nil, ErrPayloadCannotBeSerialized
	}

	return payloadName, data, nil
}

// CreatePayload reconstructs a payload based on it's type and the json data
func (p *JSONPayloadTransformer) CreatePayload(typeName string, data interface{}) (interface{}, error) {
	var dataBytes []byte
	switch d := data.(type) {
	case []byte:
		dataBytes = d
	case json.RawMessage:
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
	payload := payloadType.initiator()

	// Pointer we can handle nicely
	if payloadType.isPtr {
		if err := internal.UnmarshalJSON(dataBytes, payload); err != nil {
			return nil, err
		}
	}

	// Not a pointer so let's cry and use reflection
	vp := reflect.New(payloadType.reflectionType)
	vp.Elem().Set(reflect.ValueOf(payload))
	if err := internal.UnmarshalJSON(dataBytes, vp.Interface()); err != nil {
		return nil, err
	}

	return vp.Elem().Interface(), nil
}
