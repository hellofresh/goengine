package reflection

import (
	"reflect"

	"github.com/hellofresh/goengine/errors"
)

// TypeRegistry is a registry for go types
// this is necessary since we can't create a type from
// a string and it's json. With this registry we can
// know how to create a type for that string
type TypeRegistry interface {
	Register(i interface{}) error
	Unregister(i interface{}) error
	Exists(name string) bool
	Get(name string) (interface{}, error)
	TypeOf(i interface{}) (reflect.Type, error)
}

// InMemoryTypeRegistry implements the in memory strategy
// for the registry
type InMemoryTypeRegistry struct {
	types map[string]reflect.Type
}

// NewInMemmoryTypeRegistry creates a new in memory registry
func NewInMemmoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{make(map[string]reflect.Type)}
}

// Register adds a type in the registry
func (tr *InMemoryTypeRegistry) Register(i interface{}) error {
	t, err := tr.TypeOf(i)
	if nil != err {
		return err
	}

	tr.types[t.String()] = t
	return nil
}

// Unregister removes a type from the registry
func (tr *InMemoryTypeRegistry) Unregister(i interface{}) error {
	t, err := tr.TypeOf(i)
	if nil != err {
		return err
	}

	delete(tr.types, t.String())
	return nil
}

// Exists checks if a type exists in the registry
func (tr *InMemoryTypeRegistry) Exists(name string) bool {
	_, exists := tr.types[name]
	return exists
}

// Get retrieves a reflect.Type based on a name
func (tr *InMemoryTypeRegistry) Get(name string) (interface{}, error) {
	if typ, ok := tr.types[name]; ok {
		return reflect.New(typ).Interface(), nil
	}
	return nil, errors.ErrorTypeNotFound
}

// TypeOf returns the type of a struct checking if it's a pointer or not
func (tr *InMemoryTypeRegistry) TypeOf(i interface{}) (reflect.Type, error) {
	// Convert the interface i to a reflect.Type t
	t := reflect.TypeOf(i)
	// Check if the input is a pointer and dereference it if yes
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// Check if the input is a struct
	if t.Kind() != reflect.Struct {
		return nil, errors.ErrorTypeNotStruct
	}

	return t, nil
}
