package goengine

type EventStore interface {
	Append(events *EventStream) error
	GetEventsFor(streamName StreamName, id string) (*EventStream, error)
	FromVersion(streamName StreamName, id string, version int) (*EventStream, error)
	CountEventsFor(streamName StreamName, id string) (int, error)
}
