package goengine

import (
	"reflect"

	
	"github.com/hellofresh/goengine/errors"
	"github.com/hellofresh/goengine/reflection"
)

// TypeRegistry is a registry for go types
// this is necessary since we can't create a type from
// a string and it's json. With this registry we can
// know how to create a type for that string
type TypeRegistry interface {
	GetTypeByName(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
	RegisterEvents(events ...interface{})
	RegisterType(interface{})
	Get(name string) (interface{}, error)
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

// RegisterType adds a type in the registry
func (r *InMemoryTypeRegistry) RegisterType(i interface{}) {
	rawType := reflection.TypeOf(i)
	r.types[rawType.String()] = rawType
}

func (r *InMemoryTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	r.RegisterEvents(events)
}

func (r *InMemoryTypeRegistry) RegisterEvents(events ...interface{}) {
	for _, event := range events {
		r.RegisterType(event)
	}
}

func (r *InMemoryTypeRegistry) GetTypeByName(typeName string) (reflect.Type, bool) {
	if typeValue, ok := r.types[typeName]; ok {
		return typeValue, ok
	}

	return nil, false
}

// Get retrieves a reflect.Type based on a name
func (r *InMemoryTypeRegistry) Get(name string) (interface{}, error) {
	if typ, ok := r.GetTypeByName(name); ok {
		return reflect.New(typ).Interface(), nil
	}
	return nil, errors.ErrorTypeNotFound
}
