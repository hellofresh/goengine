package eventsourcing

import (
	"reflect"
	"time"

	"github.com/hellofresh/goengine/messaging"
)

type (
	// AggregateChanged is an outer event that indicates that a aggregate was changed
	AggregateChanged interface {
		messaging.Message

		// AggregateID return the aggregate id
		AggregateID() AggregateID
		// Version return the aggregate version
		Version() int
		// withVersion returns the AggregateChanged with the aggregate version set
		withVersion(version int) AggregateChanged
	}

	aggregateChanged struct {
		uuid        messaging.UUID
		aggregateID AggregateID
		payload     interface{}
		metadata    map[string]interface{}
		createdAt   time.Time
		version     int
	}
)

// NewAggregateChange created a new immutable aggregateChanged message
func NewAggregateChange(aggregateID AggregateID, event interface{}) AggregateChanged {
	return &aggregateChanged{
		uuid:        messaging.GenerateUUID(),
		aggregateID: aggregateID,
		payload:     event,
		createdAt:   timeNow(),
		metadata:    map[string]interface{}{},
	}
}

// UUID returns the unique message identifier
func (a *aggregateChanged) UUID() messaging.UUID {
	return a.uuid
}

func (a *aggregateChanged) AggregateID() AggregateID {
	return a.aggregateID
}

func (a *aggregateChanged) CreatedAt() time.Time {
	return a.createdAt
}

func (a *aggregateChanged) Version() int {
	return a.version
}

func (a *aggregateChanged) Payload() interface{} {
	return a.payload
}

func (a *aggregateChanged) Metadata() map[string]interface{} {
	metadataCopy := make(map[string]interface{}, len(a.metadata))
	for k, v := range a.metadata {
		metadataCopy[k] = v
	}

	return metadataCopy
}

func (a *aggregateChanged) WithAddedMetadata(key string, value interface{}) messaging.Message {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String:
		// The above kinds are allowed as metadata values
		// If you wish to use some struct or other complex type please serialize it first
	default:
		panic("unsupported metadata value, only scalar types are allowed")
	}

	newMetadata := make(map[string]interface{}, len(a.metadata)+1)
	newMetadata[key] = value
	for k, v := range a.metadata {
		newMetadata[k] = v
	}

	newAggregateChanged := *a
	newAggregateChanged.metadata = newMetadata

	return &newAggregateChanged
}

func (a *aggregateChanged) withVersion(version int) AggregateChanged {
	newAggregateChanged := *a
	newAggregateChanged.version = version

	return &newAggregateChanged
}
