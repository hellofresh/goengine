package internal

import (
	"encoding/json"

	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jwriter"
)

// MarshalJSON returns the JSON encoding of v.
// This is done using `easyjson.Marshaler`, `json.Marshaler` or `json.Marshal`
func MarshalJSON(v interface{}) ([]byte, error) {
	if vm, ok := v.(easyjson.Marshaler); ok {
		w := &jwriter.Writer{}
		vm.MarshalEasyJSON(w)
		return w.Buffer.BuildBytes(), w.Error
	}

	if vm, ok := v.(json.Marshaler); ok {
		return vm.MarshalJSON()
	}

	return json.Marshal(v)
}

// UnmarshalJSON parses the JSON-encoded data and stores the result
// in the value pointed to by v.
// This is done using `easyjson.Unmarshal`, `json.Unmarshaler` or `json.Unmarshal`
func UnmarshalJSON(data []byte, v interface{}) error {
	if vm, ok := v.(easyjson.Unmarshaler); ok {
		return easyjson.Unmarshal(data, vm)
	}

	if vm, ok := v.(json.Unmarshaler); ok {
		return vm.UnmarshalJSON(data)
	}

	return json.Unmarshal(data, v)
}
