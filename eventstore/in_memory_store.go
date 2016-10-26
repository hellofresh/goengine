package eventstore

type InMemoryEventStore struct {
	events map[StreamName]map[string][]*DomainMessage
}

func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{make(map[StreamName]map[string][]*DomainMessage)}
}

func (s *InMemoryEventStore) Save(streamName StreamName, event *DomainMessage) {
	id := event.ID
	events, exists := s.events[streamName][id]

	if !exists {
		s.events[streamName] = make(map[string][]*DomainMessage)
	}

	s.events[streamName][id] = append(events, event)
}

func (s *InMemoryEventStore) GetEventsFor(streamName StreamName, id string) []*DomainMessage {
	return s.events[streamName][id]
}

func (s *InMemoryEventStore) FromVersion(streamName StreamName, id string, version int) []*DomainMessage {
	events := s.GetEventsFor(streamName, id)
	var filtered []*DomainMessage

	for _, event := range events {
		if event.version >= version {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

func (s *InMemoryEventStore) CountEventsFor(streamName StreamName, id string) int {
	return len(s.GetEventsFor(streamName, id))
}
