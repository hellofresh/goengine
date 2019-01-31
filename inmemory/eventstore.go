package inmemory

import "github.com/hellofresh/goengine"

// EventStore ...
type EventStore struct {
	events map[goengine.StreamName]map[string][]*goengine.DomainMessage
}

// NewEventStore ...
func NewEventStore() *EventStore {
	return &EventStore{make(map[goengine.StreamName]map[string][]*goengine.DomainMessage)}
}

// Append ...
func (s *EventStore) Append(events *goengine.EventStream) error {
	name := events.Name
	for _, event := range events.Events {
		err := s.save(name, event)
		if nil != err {
			return err
		}
	}

	return nil
}

// GetEventsFor ...
func (s *EventStore) GetEventsFor(streamName goengine.StreamName, id string) (*goengine.EventStream, error) {
	return goengine.NewEventStream(streamName, s.events[streamName][id]), nil
}

// FromVersion ...
func (s *EventStore) FromVersion(streamName goengine.StreamName, id string, version int) (*goengine.EventStream, error) {
	events, _ := s.GetEventsFor(streamName, id)
	var filtered []*goengine.DomainMessage

	for _, event := range events.Events {
		if event.Version >= version {
			filtered = append(filtered, event)
		}
	}

	return goengine.NewEventStream(streamName, filtered), nil
}

// CountEventsFor ...
func (s *EventStore) CountEventsFor(streamName goengine.StreamName, id string) (int64, error) {
	stream, _ := s.GetEventsFor(streamName, id)
	return int64(len(stream.Events)), nil
}

func (s *EventStore) save(streamName goengine.StreamName, event *goengine.DomainMessage) error {
	id := event.ID
	events, exists := s.events[streamName][id]

	if !exists {
		s.events[streamName] = make(map[string][]*goengine.DomainMessage)
	}

	s.events[streamName][id] = append(events, event)

	return nil
}
