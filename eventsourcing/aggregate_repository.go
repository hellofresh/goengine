package eventsourcing

import (
	log "github.com/Sirupsen/logrus"
	"github.com/hellofresh/goengine/eventstore"
)

type AggregateRepository interface {
	Load(string, eventstore.StreamName) (*eventstore.EventStream, error)
	Save(AggregateRoot, eventstore.StreamName)
	Reconstitute(string, AggregateRoot, eventstore.StreamName) error
}

type PublisherRepository struct {
	eventStore eventstore.EventStore
}

func NewPublisherRepository(eventStore eventstore.EventStore) *PublisherRepository {
	return &PublisherRepository{eventStore}
}

func (r *PublisherRepository) Load(id string, streamName eventstore.StreamName) (*eventstore.EventStream, error) {
	log.Debugf("Loading events from %s stream for aggregate %s", streamName, id)
	stream, err := r.eventStore.GetEventsFor(streamName, id)
	if nil != err {
		return nil, err
	}

	return stream, nil
}

func (r *PublisherRepository) Save(aggregateRoot AggregateRoot, streamName eventstore.StreamName) {
	events := aggregateRoot.GetUncommittedEvents()
	eventStream := eventstore.NewEventStream(streamName, events)
	log.Debugf("Saving %d events to %s stream", len(events), streamName)
	r.eventStore.Append(eventStream)
}

func (r *PublisherRepository) Reconstitute(id string, source AggregateRoot, streamName eventstore.StreamName) error {
	log.Debugf("Reconstituting aggregate %s from %s stream", id, streamName)
	stream, err := r.Load(id, streamName)
	if nil != err {
		return err
	}
	events := stream.Events

	for _, event := range events {
		source.Apply(event.Payload)
	}

	source.SetVersion(events[len(events)-1].Version)
	log.Debugf("Aggregate %s reconstituted", id)
	return nil
}
