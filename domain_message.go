package goengine

import (
	"fmt"
	"time"
)

// DomainEvent ...
type DomainEvent interface {
	OccurredOn() time.Time
}

// DomainMessage ...
type DomainMessage struct {
	ID         string      `json:"aggregate_id,omitempty"`
	Version    int         `json:"version"`
	Payload    DomainEvent `json:"payload"`
	RecordedOn time.Time   `json:"recorded_on"`
}

// String ...
func (dm *DomainMessage) String() string {
	return fmt.Sprintf("DomainMessage{ ID: %s, Version: %d }", dm.ID, dm.Version)
}

// NewDomainMessage ...
func NewDomainMessage(id string, version int, payload DomainEvent, recordedOn time.Time) *DomainMessage {
	return &DomainMessage{id, version, payload, recordedOn}
}

// RecordNow ...
func RecordNow(id string, version int, payload DomainEvent) *DomainMessage {
	recordedTime := time.Now()
	return NewDomainMessage(id, version, payload, recordedTime)
}
