package eventstore

import "time"

type DomainMessage struct {
	ID         string
	version    int
	payload    DomainEvent
	recordedOn time.Time
}

func NewDomainMessage(id string, version int, payload DomainEvent, recordedOn time.Time) *DomainMessage {
	return &DomainMessage{id, version, payload, recordedOn}
}

func RecordNow(id string, version int, payload DomainEvent) *DomainMessage {
	recordedTime := time.Now()
	return NewDomainMessage(id, version, payload, recordedTime)
}
