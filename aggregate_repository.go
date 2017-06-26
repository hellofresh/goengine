package goengine

import (
	"fmt"
)

type AggregateRepository interface {
	Load(string, StreamName) (*EventStream, error)
	Save(AggregateRoot, StreamName) error
	Reconstitute(string, AggregateRoot, StreamName) error
}

type PublisherRepository struct {
	EventStore EventStore
	EventBus   VersionedEventPublisher
}

func NewPublisherRepository(eventStore EventStore, eventBus VersionedEventPublisher) *PublisherRepository {
	return &PublisherRepository{eventStore, eventBus}
}

func (r *PublisherRepository) Load(id string, streamName StreamName) (*EventStream, error) {
	Log("Loading events from stream for aggregate", map[string]interface{}{"stream": streamName, "id": id}, nil)
	stream, err := r.EventStore.GetEventsFor(streamName, id)
	if nil != err {
		return nil, err
	}

	return stream, nil
}

func (r *PublisherRepository) Save(aggregateRoot AggregateRoot, streamName StreamName) error {
	events := aggregateRoot.GetUncommittedEvents()
	eventStream := NewEventStream(streamName, events)
	Log("Saving events to stream", map[string]interface{}{"count": len(events), "stream": streamName}, nil)

	err := r.EventStore.Append(eventStream)
	if nil != err {
		return err
	}

	if nil == r.EventBus {
		Log("Event bus not detected, skipping publishing events", nil, nil)
		return nil
	}

	if err = r.EventBus.PublishEvents(events); err != nil {
		return err
	}

	return nil
}

func (r *PublisherRepository) Reconstitute(id string, source AggregateRoot, streamName StreamName) error {
	Log("Reconstituting aggregate from stream", map[string]interface{}{"stream": streamName, "id": id}, nil)

	stream, err := r.Load(id, streamName)
	if nil != err {
		return err
	}
	events := stream.Events

	if len(events) == 0 {
		return fmt.Errorf("No events found for id: %s", id)
	}

	for _, event := range events {
		source.Apply(event.Payload)
	}

	source.SetVersion(events[len(events)-1].Version)
	Log("Aggregate reconstituted", map[string]interface{}{"id": id}, nil)
	return nil
}
