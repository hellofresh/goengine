package goengine

// StreamName ...
type StreamName string

// EventStream ...
type EventStream struct {
	Name   StreamName
	Events []*DomainMessage
}

// NewEventStream ...
func NewEventStream(name StreamName, events []*DomainMessage) *EventStream {
	return &EventStream{name, events}
}
