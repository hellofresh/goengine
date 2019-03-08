package metadata

import (
	"encoding/json"

	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pkg/errors"
)

type (
	// Metadata is an immutable map[string]interface{} implementation
	Metadata interface {
		// Value returns the value associated with this context for key, or nil
		// if no value is associated with key. Successive calls to Value with
		// the same key returns the same result.
		Value(key string) interface{}

		// AsMap return the Metadata as a map[string]interface{}
		AsMap() map[string]interface{}
	}

	// jsonMarshaler is an interface used to speed up the json marshalling process
	jsonMarshaler interface {
		// MarshalJSONKeyValue writes the Metadata Key and Value and all it's parent Key Value pairs into the writer
		// If last is true it MUST omit the `,` from the last Key Value pair
		MarshalJSONKeyValue(out *jwriter.Writer, last bool)
	}
)

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
	// Ensure valueData implements the jsonMarshaler interface
	_ jsonMarshaler = new(emptyData)
	// Ensure emptyData implements the json.Marshaler interface
	_ json.Marshaler = new(emptyData)
	// Ensure emptyData implements the easyjson.Marshaler interface
	_ easyjson.Marshaler = new(emptyData)
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

func (v *emptyData) MarshalEasyJSON(w *jwriter.Writer) {
	w.RawByte('{')
	w.RawByte('}')
}

func (v *emptyData) MarshalJSONKeyValue(*jwriter.Writer, bool) {
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
	// Ensure valueData implements the jsonMarshaler interface
	_ jsonMarshaler = new(valueData)
	// Ensure valueData implements the json.Marshaler interface
	_ json.Marshaler = new(valueData)
	// Ensure valueData implements the easyjson.Marshaler interface
	_ easyjson.Marshaler = new(valueData)
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
	w := jwriter.Writer{}
	v.MarshalEasyJSON(&w)
	return w.Buffer.BuildBytes(), w.Error
}

func (v *valueData) MarshalEasyJSON(w *jwriter.Writer) {
	w.RawByte('{')
	v.MarshalJSONKeyValue(w, true)
	w.RawByte('}')
}

func (v *valueData) MarshalJSONKeyValue(out *jwriter.Writer, last bool) {
	marshalJSONKeyValues(out, v.Metadata)

	out.String(v.key)
	out.RawByte(':')
	if vm, ok := v.val.(easyjson.Marshaler); ok {
		vm.MarshalEasyJSON(out)
	} else if vm, ok := v.val.(json.Marshaler); ok {
		out.Raw(vm.MarshalJSON())
	} else {
		out.Raw(json.Marshal(v.val))
	}

	if !last {
		out.RawByte(',')
	}
}

// UnmarshalJSON unmarshals the provided json into a Metadata instance
func UnmarshalJSON(json []byte) (Metadata, error) {
	in := jlexer.Lexer{Data: json}
	metadata := New()

	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return metadata, in.Error()
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

	return metadata, in.Error()
}

func marshalJSONKeyValues(out *jwriter.Writer, parent Metadata) {
	if parent == nil {
		return
	}

	if m, ok := parent.(jsonMarshaler); ok {
		m.MarshalJSONKeyValue(out, false)
		return
	}

	var (
		parentJSON []byte
		err        error
	)
	if vm, ok := parent.(easyjson.Marshaler); ok {
		w := &jwriter.Writer{}
		vm.MarshalEasyJSON(out)
		parentJSON = w.Buffer.BuildBytes()
		err = w.Error
	} else if vm, ok := parent.(json.Marshaler); ok {
		parentJSON, err = vm.MarshalJSON()
	} else {
		parentJSON, err = json.Marshal(parent)
	}

	if err != nil {
		out.Raw(parentJSON, err)
		return
	}

	plen := len(parentJSON)
	if parentJSON[1] != '{' || parentJSON[plen-1] != '}' {
		out.Raw(parentJSON, errors.Errorf("JSON unmarshal failed for Metadata of type %T", parent))
		return
	}

	out.Raw(parentJSON[1:plen-2], nil)
}
