package eventsourcing

import (
	"fmt"
	"reflect"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/reflection"
	"github.com/pborman/uuid"
)

type AggregateRoot struct {
	ID               string
	version          int
	source           interface{}
	uncommitedEvents []*eventstore.DomainMessage
}

// NewAggregateRoot constructor
func NewAggregateRoot(source interface{}) AggregateRoot {
	return NewEventSourceBasedWithID(source, uuid.New())
}

// NewEventSourceBasedWithID constructor
func NewEventSourceBasedWithID(source interface{}, id string) AggregateRoot {
	return AggregateRoot{id, 0, source, []*eventstore.DomainMessage{}}
}

func (ar *AggregateRoot) ReconstituteFromHistory(historyEvents *eventstore.EventStream) {

}

func (ar *AggregateRoot) GetUncommittedEvents() []*eventstore.DomainMessage {
	stream := ar.uncommitedEvents
	ar.uncommitedEvents = nil

	return stream
}

func (r *AggregateRoot) Apply(event eventstore.DomainEvent) {
	t := reflect.TypeOf(event)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	reflection.CallMethod(r.source, fmt.Sprintf("When%s", t.Name()), event)
}

func (r *AggregateRoot) RecordThat(event eventstore.DomainEvent) {
	r.version++
	r.Apply(event)
	r.Record(event)
}

func (r *AggregateRoot) Replay(historyEvents *eventstore.EventStream, version int) {
	for _, event := range historyEvents.Events {
		r.version = event.Version
		r.Apply(event.Payload)
	}
}

func (ar *AggregateRoot) Record(event eventstore.DomainEvent) {
	message := eventstore.RecordNow(ar.ID, ar.version, event)
	ar.uncommitedEvents = append(ar.uncommitedEvents, message)
}
