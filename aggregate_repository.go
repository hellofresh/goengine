package goengine

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
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
	log.Debugf("Loading events from %s stream for aggregate %s", streamName, id)
	stream, err := r.EventStore.GetEventsFor(streamName, id)
	if nil != err {
		return nil, err
	}

	return stream, nil
}

func (r *PublisherRepository) Save(aggregateRoot AggregateRoot, streamName StreamName) error {
	events := aggregateRoot.GetUncommittedEvents()
	eventStream := NewEventStream(streamName, events)
	log.Debugf("Saving %d events to %s stream", len(events), streamName)
	err := r.EventStore.Append(eventStream)
	if nil != err {
		return err
	}

	if nil == r.EventBus {
		log.Debug("Event bus not detected, skiping publishing events")
		return nil
	}

	if err = r.EventBus.PublishEvents(events); err != nil {
		return err
	}

	return nil
}

func (r *PublisherRepository) Reconstitute(id string, source AggregateRoot, streamName StreamName) error {
	log.Debugf("Reconstituting aggregate %s from %s stream", id, streamName)
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
	log.Debugf("Aggregate %s reconstituted", id)
	return nil
}
