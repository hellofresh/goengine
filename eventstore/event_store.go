package eventstore

type EventStore interface {
	Append(events *EventStream) error
	GetEventsFor(streamName StreamName, id string) (*EventStream, error)
	FromVersion(streamName StreamName, id string, version int) (*EventStream, error)
	CountEventsFor(streamName StreamName, id string) (int, error)
}

type EventStoreAdapter interface {
	Save(streamName StreamName, events *DomainMessage) error
	GetEventsFor(streamName StreamName, id string) ([]*DomainMessage, error)
	FromVersion(streamName StreamName, id string, version int) ([]*DomainMessage, error)
	CountEventsFor(streamName StreamName, id string) (int, error)
}

type EventStoreImp struct {
	adapter EventStoreAdapter
}

func NewEventStore(adapter EventStoreAdapter) *EventStoreImp {
	return &EventStoreImp{adapter}
}

func (es *EventStoreImp) Append(events *EventStream) error {
	name := events.Name
	for _, event := range events.Events {
		err := es.adapter.Save(name, event)
		if nil != err {
			return err
		}
	}

	return nil
}

func (es *EventStoreImp) FromVersion(streamName StreamName, id string, version int) (*EventStream, error) {
	events, err := es.adapter.FromVersion(streamName, id, version)
	return NewEventStream(streamName, events), err
}

func (es *EventStoreImp) GetEventsFor(streamName StreamName, id string) (*EventStream, error) {
	events, err := es.adapter.GetEventsFor(streamName, id)
	return NewEventStream(streamName, events), err
}

func (es *EventStoreImp) CountEventsFor(streamName StreamName, id string) (int, error) {
	return es.adapter.CountEventsFor(streamName, id)
}
