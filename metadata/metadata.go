package metadata

type (
	// Metadata is an immutable map[string]interface{} implementation
	Metadata interface {
		// Value returns the value associated with this context for key, or nil
		// if no value is associated with key. Successive calls to Value with
		// the same key returns the same result.
		Value(key string) interface{}
	}

	emptyData int

	valueData struct {
		Metadata
		key string
		val interface{}
	}
)

// New return a new Metadata instance without any information
func New() Metadata {
	return new(emptyData)
}

// WithValue returns a copy of parent in which the value associated with key is val.
func WithValue(parent Metadata, key string, val interface{}) Metadata {
	return &valueData{parent, key, val}
}

// AsMap return the Metadata as a map[string]interface{}
//
// Warning: If you are using a custom implementation of Metadata this will panic
func AsMap(metadata Metadata) map[string]interface{} {
	res := map[string]interface{}{}
	for {
		switch m := metadata.(type) {
		case nil,
			*emptyData:
			return res
		case *valueData:
			if _, exists := res[m.key]; !exists {
				res[m.key] = m.val
			}

			metadata = m.Metadata
		default:
			panic("unsupported Metadata type")
		}
	}
}

func (*emptyData) Value(key string) interface{} {
	return nil
}

func (v *valueData) Value(key string) interface{} {
	if v.key == key {
		return v.val
	}

	return v.Metadata.Value(key)
}
