package inmemory

import "github.com/hellofresh/goengine/eventstore"

type InMemoryEventStore struct {
	events map[eventstore.StreamName]map[string][]*eventstore.DomainMessage
}

func NewEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{make(map[eventstore.StreamName]map[string][]*eventstore.DomainMessage)}
}

func (s *InMemoryEventStore) Append(events *eventstore.EventStream) error {
	name := events.Name
	for _, event := range events.Events {
		err := s.save(name, event)
		if nil != err {
			return err
		}
	}

	return nil
}

func (s *InMemoryEventStore) GetEventsFor(streamName eventstore.StreamName, id string) (*eventstore.EventStream, error) {
	return eventstore.NewEventStream(streamName, s.events[streamName][id]), nil
}

func (s *InMemoryEventStore) FromVersion(streamName eventstore.StreamName, id string, version int) (*eventstore.EventStream, error) {
	events, _ := s.GetEventsFor(streamName, id)
	var filtered []*eventstore.DomainMessage

	for _, event := range events.Events {
		if event.Version >= version {
			filtered = append(filtered, event)
		}
	}

	return eventstore.NewEventStream(streamName, filtered), nil
}

func (s *InMemoryEventStore) CountEventsFor(streamName eventstore.StreamName, id string) (int, error) {
	stream, _ := s.GetEventsFor(streamName, id)
	return len(stream.Events), nil
}

func (s *InMemoryEventStore) save(streamName eventstore.StreamName, event *eventstore.DomainMessage) error {
	id := event.ID
	events, exists := s.events[streamName][id]

	if !exists {
		s.events[streamName] = make(map[string][]*eventstore.DomainMessage)
	}

	s.events[streamName][id] = append(events, event)

	return nil
}
