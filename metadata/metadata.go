package metadata

import (
	"encoding/json"

	"github.com/mailru/easyjson/jlexer"
)

// Metadata is an immutable map[string]interface{} implementation
type Metadata interface {
	// Value returns the value associated with this context for key, or nil
	// if no value is associated with key. Successive calls to Value with
	// the same key returns the same result.
	Value(key string) interface{}

	// AsMap return the Metadata as a map[string]interface{}
	AsMap() map[string]interface{}
}

// New return a new Metadata instance without any information
func New() Metadata {
	return new(emptyData)
}

// FromMap returns a new Metadata instance filled with the map data
func FromMap(data map[string]interface{}) Metadata {
	meta := New()
	for k, v := range data {
		meta = WithValue(meta, k, v)
	}

	return meta
}

// WithValue returns a copy of parent in which the value associated with key is val.
func WithValue(parent Metadata, key string, val interface{}) Metadata {
	return &valueData{parent, key, val}
}

// emptyData represents the empty root of a metadata chain
type emptyData int

var (
	// Ensure emptyData implements the Metadata interface
	_ Metadata = new(emptyData)
	// Ensure valueData implements the json.Marshaler interface
	_ json.Marshaler = new(emptyData)
)

func (*emptyData) Value(key string) interface{} {
	return nil
}

func (*emptyData) AsMap() map[string]interface{} {
	return map[string]interface{}{}
}

func (v *emptyData) MarshalJSON() ([]byte, error) {
	return []byte("{}"), nil
}

// valueData represents a key, value pair in a metadata chain
type valueData struct {
	Metadata
	key string
	val interface{}
}

var (
	// Ensure valueData implements the Metadata interface
	_ Metadata = new(valueData)
	// Ensure valueData implements the json.Marshaler interface
	_ json.Marshaler = new(valueData)
)

func (v *valueData) Value(key string) interface{} {
	if v.key == key {
		return v.val
	}

	return v.Metadata.Value(key)
}

func (v *valueData) AsMap() map[string]interface{} {
	var m map[string]interface{}
	if v.Metadata == nil {
		m = map[string]interface{}{}
	} else {
		m = v.Metadata.AsMap()
	}

	m[v.key] = v.val

	return m
}

func (v *valueData) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.AsMap())
}

// JSONMetadata is a special struct to UnmarshalJSON metadata
type JSONMetadata struct {
	Metadata Metadata
}

var (
	// Ensure JSONMetadata implements the json.Marshaler interface
	_ json.Marshaler = &JSONMetadata{}
	// Ensure JSONMetadata implements the json.Unmarshaler interface
	_ json.Unmarshaler = &JSONMetadata{}
)

// MarshalJSON returns a json representation of the wrapped Metadata
func (j JSONMetadata) MarshalJSON() ([]byte, error) {
	if j.Metadata == nil {
		j.Metadata = New()
	}

	return json.Marshal(j.Metadata)
}

// UnmarshalJSON unmarshal the json into Metdadata
func (j *JSONMetadata) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	j.UnmarshalEasyJSON(&r)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (j *JSONMetadata) UnmarshalEasyJSON(in *jlexer.Lexer) {
	metadata := New()

	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		metadata = WithValue(metadata, key, in.Interface())
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}

	j.Metadata = metadata
}
