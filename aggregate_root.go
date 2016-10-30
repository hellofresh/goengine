package goengine

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
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
	ID               string
	version          int
	source           interface{}
	uncommitedEvents []*DomainMessage
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
	stream := r.uncommitedEvents
	r.uncommitedEvents = nil
	log.Debugf("%d Uncommited events cleaned", len(stream))

	return stream
}

func (r *AggregateRootBased) Apply(event DomainEvent) {
	t := reflection.TypeOf(event)
	reflection.CallMethod(r.source, fmt.Sprintf("When%s", t.Name()), event)
	log.Debugf("Event %s applied", t.Name())
}

func (r *AggregateRootBased) RecordThat(event DomainEvent) {
	r.version++
	r.Apply(event)
	r.Record(event)
}

func (r *AggregateRootBased) Record(event DomainEvent) {
	message := RecordNow(r.ID, r.version, event)
	r.uncommitedEvents = append(r.uncommitedEvents, message)
	log.Debug("Event recorded")
}
