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
	RegisterAggregate(AggregateRoot, ...interface{})
	RegisterEvents(...interface{})
	RegisterType(interface{})
	Get(string) (interface{}, error)
}

// InMemoryTypeRegistry implements the in memory strategy
// for the registry
type InMemoryTypeRegistry struct {
	types map[string]reflect.Type
}

// NewInMemoryTypeRegistry creates a new in memory registry
func NewInMemoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{make(map[string]reflect.Type)}
}

// RegisterType adds a type in the registry
func (r *InMemoryTypeRegistry) RegisterType(i interface{}) {
	rawType := reflection.TypeOf(i)
	r.types[rawType.String()] = rawType
	Log("Type was registered", map[string]interface{}{"type": rawType.String()}, nil)
}

// RegisterAggregate ...
func (r *InMemoryTypeRegistry) RegisterAggregate(aggregate AggregateRoot, events ...interface{}) {
	r.RegisterType(aggregate)

	fields := map[string]interface{}{"aggregate": aggregate.GetID()}
	Log("Aggregate was registered", fields, nil)

	r.RegisterEvents(events)
	fields["count"] = len(events)
	Log("Events were registered for aggregate", fields, nil)
}

// RegisterEvents ...
func (r *InMemoryTypeRegistry) RegisterEvents(events ...interface{}) {
	for _, event := range events {
		r.RegisterType(event)
	}
}

// GetTypeByName ...
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

	Log("Type not found", map[string]interface{}{"type": name}, nil)
	return nil, errors.ErrorTypeNotFound
}
