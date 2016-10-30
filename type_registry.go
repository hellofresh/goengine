package goengine

import (
	"reflect"

	log "github.com/Sirupsen/logrus"
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

// NewInMemmoryTypeRegistry creates a new in memory registry
func NewInMemmoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{make(map[string]reflect.Type)}
}

// RegisterType adds a type in the registry
func (r *InMemoryTypeRegistry) RegisterType(i interface{}) {
	rawType := reflection.TypeOf(i)
	r.types[rawType.String()] = rawType
	log.Debugf("Type %s was registered", rawType.String())
}

func (r *InMemoryTypeRegistry) RegisterAggregate(aggregate AggregateRoot, events ...interface{}) {
	r.RegisterType(aggregate)
	log.Debugf("Aggregate %s was registered", aggregate.GetID())

	r.RegisterEvents(events)
	log.Debugf("%s events were registered for aggregate %s", len(events), aggregate.GetID())
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

	log.Debugf("Type %s not found", name)
	return nil, errors.ErrorTypeNotFound
}
