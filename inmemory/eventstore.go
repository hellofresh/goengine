package inmemory

import "github.com/hellofresh/goengine/eventstore"

type InMemoryEventStore struct {
	events map[eventstore.StreamName]map[string][]*eventstore.DomainMessage
}

func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{make(map[eventstore.StreamName]map[string][]*eventstore.DomainMessage)}
}

func (s *InMemoryEventStore) Save(streamName eventstore.StreamName, event *eventstore.DomainMessage) error {
	id := event.ID
	events, exists := s.events[streamName][id]

	if !exists {
		s.events[streamName] = make(map[string][]*eventstore.DomainMessage)
	}

	s.events[streamName][id] = append(events, event)

	return nil
}

func (s *InMemoryEventStore) GetEventsFor(streamName eventstore.StreamName, id string) ([]*eventstore.DomainMessage, error) {
	return s.events[streamName][id], nil
}

func (s *InMemoryEventStore) FromVersion(streamName eventstore.StreamName, id string, version int) ([]*eventstore.DomainMessage, error) {
	events, _ := s.GetEventsFor(streamName, id)
	var filtered []*eventstore.DomainMessage

	for _, event := range events {
		if event.Version >= version {
			filtered = append(filtered, event)
		}
	}

	return filtered, nil
}

func (s *InMemoryEventStore) CountEventsFor(streamName eventstore.StreamName, id string) (int, error) {
	result, _ := s.GetEventsFor(streamName, id)
	return len(result), nil
}
