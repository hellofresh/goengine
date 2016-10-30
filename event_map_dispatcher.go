package goengine

import (
	"reflect"
)

// MapBasedVersionedEventDispatcher is a simple implementation of the versioned event dispatcher. Using a map it registered event handlers to event types
type MapBasedVersionedEventDispatcher struct {
	registry       map[reflect.Type][]VersionedEventHandler
	globalHandlers []VersionedEventHandler
}

// NewVersionedEventDispatcher is a constructor for the MapBasedVersionedEventDispatcher
func NewVersionedEventDispatcher() *MapBasedVersionedEventDispatcher {
	registry := make(map[reflect.Type][]VersionedEventHandler)
	return &MapBasedVersionedEventDispatcher{registry, []VersionedEventHandler{}}
}

// RegisterEventHandler allows a caller to register an event handler given an event of the specified type being received
func (m *MapBasedVersionedEventDispatcher) RegisterEventHandler(event interface{}, handler VersionedEventHandler) {
	eventType := reflect.TypeOf(event)
	handlers, ok := m.registry[eventType]
	if ok {
		m.registry[eventType] = append(handlers, handler)
	} else {
		m.registry[eventType] = []VersionedEventHandler{handler}
	}
}

// RegisterGlobalHandler allows a caller to register a wildcard event handler call on any event received
func (m *MapBasedVersionedEventDispatcher) RegisterGlobalHandler(handler VersionedEventHandler) {
	m.globalHandlers = append(m.globalHandlers, handler)
}

// DispatchEvent executes all event handlers registered for the given event type
func (m *MapBasedVersionedEventDispatcher) DispatchEvent(event *DomainMessage) error {
	eventType := reflect.TypeOf(event.Payload)
	if handlers, ok := m.registry[eventType]; ok {
		for _, handler := range handlers {
			if err := handler(event); err != nil {
				return err
			}
		}
	}

	for _, handler := range m.globalHandlers {
		if err := handler(event); err != nil {
			return err
		}
	}

	return nil
}
