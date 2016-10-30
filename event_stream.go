package goengine

type StreamName string

type EventStream struct {
	Name   StreamName
	Events []*DomainMessage
}

func NewEventStream(name StreamName, events []*DomainMessage) *EventStream {
	return &EventStream{name, events}
}
