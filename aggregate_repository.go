package goengine

import (
	"fmt"

	log "github.com/sirupsen/logrus"
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
	log.WithFields(log.Fields{"stream": streamName, "id": id}).Debug("Loading events from stream for aggregate")
	stream, err := r.EventStore.GetEventsFor(streamName, id)
	if nil != err {
		return nil, err
	}

	return stream, nil
}

func (r *PublisherRepository) Save(aggregateRoot AggregateRoot, streamName StreamName) error {
	events := aggregateRoot.GetUncommittedEvents()
	eventStream := NewEventStream(streamName, events)
	log.WithFields(log.Fields{"count": len(events), "stream": streamName}).Debug("Saving events to stream")
	err := r.EventStore.Append(eventStream)
	if nil != err {
		return err
	}

	if nil == r.EventBus {
		log.Debug("Event bus not detected, skipping publishing events")
		return nil
	}

	if err = r.EventBus.PublishEvents(events); err != nil {
		return err
	}

	return nil
}

func (r *PublisherRepository) Reconstitute(id string, source AggregateRoot, streamName StreamName) error {
	log.WithFields(log.Fields{"stream": streamName, "id": id}).Debug("Reconstituting aggregate from stream")
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
	log.WithField("id", id).Debug("Aggregate reconstituted")
	return nil
}
