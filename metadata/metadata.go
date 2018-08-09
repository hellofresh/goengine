package metadata

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

func (*emptyData) Value(key string) interface{} {
	return nil
}

func (*emptyData) AsMap() map[string]interface{} {
	return map[string]interface{}{}
}

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
