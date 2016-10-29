package eventstore

type InMemoryEventStore struct {
	events map[StreamName]map[string][]*DomainMessage
}

func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{make(map[StreamName]map[string][]*DomainMessage)}
}

func (s *InMemoryEventStore) Save(streamName StreamName, event *DomainMessage) error {
	id := event.ID
	events, exists := s.events[streamName][id]

	if !exists {
		s.events[streamName] = make(map[string][]*DomainMessage)
	}

	s.events[streamName][id] = append(events, event)

	return nil
}

func (s *InMemoryEventStore) GetEventsFor(streamName StreamName, id string) ([]*DomainMessage, error) {
	return s.events[streamName][id], nil
}

func (s *InMemoryEventStore) FromVersion(streamName StreamName, id string, version int) ([]*DomainMessage, error) {
	events, _ := s.GetEventsFor(streamName, id)
	var filtered []*DomainMessage

	for _, event := range events {
		if event.Version >= version {
			filtered = append(filtered, event)
		}
	}

	return filtered, nil
}

func (s *InMemoryEventStore) CountEventsFor(streamName StreamName, id string) (int, error) {
	result, _ := s.GetEventsFor(streamName, id)
	return len(result), nil
}
