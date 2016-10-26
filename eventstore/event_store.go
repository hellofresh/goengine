package eventstore

type EventStore interface {
	Append(events *EventStream)
	GetEventsFor(streamName StreamName, id string) *EventStream
	FromVersion(streamName StreamName, id string, version int) *EventStream
	CountEventsFor(streamName StreamName, id string) int
}

type EventStoreAdapter interface {
	Save(streamName StreamName, events *DomainMessage)
	GetEventsFor(streamName StreamName, id string) []*DomainMessage
	FromVersion(streamName StreamName, id string, version int) []*DomainMessage
	CountEventsFor(streamName StreamName, id string) int
}

type EventStoreImp struct {
	adapter EventStoreAdapter
}

func NewEventStore(adapter EventStoreAdapter) *EventStoreImp {
	return &EventStoreImp{adapter}
}

func (es *EventStoreImp) Append(events *EventStream) {
	name := events.Name
	for _, event := range events.Events {
		es.adapter.Save(name, event)
	}
}

func (es *EventStoreImp) FromVersion(streamName StreamName, id string, version int) *EventStream {
	events := es.adapter.FromVersion(streamName, id, version)
	return NewEventStream(streamName, events)
}

func (es *EventStoreImp) GetEventsFor(streamName StreamName, id string) *EventStream {
	events := es.adapter.GetEventsFor(streamName, id)
	return NewEventStream(streamName, events)
}

func (es *EventStoreImp) CountEventsFor(streamName StreamName, id string) int {
	return es.adapter.CountEventsFor(streamName, id)
}
