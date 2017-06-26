package goengine

import (
	"fmt"

	"github.com/hellofresh/goengine/reflection"
	"github.com/pborman/uuid"
)

type AggregateRoot interface {
	GetID() string
	GetVersion() int
	SetVersion(int)
	Apply(DomainEvent)
	GetUncommittedEvents() []*DomainMessage
}

type AggregateRootBased struct {
	ID                string
	version           int
	source            interface{}
	uncommittedEvents []*DomainMessage
}

// NewAggregateRootBased constructor
func NewAggregateRootBased(source interface{}) *AggregateRootBased {
	return NewEventSourceBasedWithID(source, uuid.New())
}

// NewEventSourceBasedWithID constructor
func NewEventSourceBasedWithID(source interface{}, id string) *AggregateRootBased {
	return &AggregateRootBased{id, 0, source, []*DomainMessage{}}
}

func (r *AggregateRootBased) GetID() string {
	return r.ID
}

func (r *AggregateRootBased) GetVersion() int {
	return r.version
}

func (r *AggregateRootBased) SetVersion(version int) {
	r.version = version
}

func (r *AggregateRootBased) GetUncommittedEvents() []*DomainMessage {
	stream := r.uncommittedEvents
	r.uncommittedEvents = nil
	Log("Uncommitted events cleaned", map[string]interface{}{"count": len(stream)}, nil)

	return stream
}

func (r *AggregateRootBased) Apply(event DomainEvent) {
	t := reflection.TypeOf(event)
	methodName := fmt.Sprintf("When%s", t.Name())

	fields := map[string]interface{}{"source": fmt.Sprintf("%+v", r.source), "method": methodName, "event": fmt.Sprintf("%+v", event)}
	Log("Applying event", fields, nil)
	reflection.CallMethod(r.source, methodName, event)
	Log("Event applied", fields, nil)
}

func (r *AggregateRootBased) RecordThat(event DomainEvent) {
	r.version++
	r.Apply(event)
	r.Record(event)
}

func (r *AggregateRootBased) Record(event DomainEvent) {
	message := RecordNow(r.ID, r.version, event)
	r.uncommittedEvents = append(r.uncommittedEvents, message)
	Log("Event recorded", nil, nil)
}
