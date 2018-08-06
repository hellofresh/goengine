package aggregate

import (
	"time"

	"github.com/google/uuid"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

type (
	// ID an UUID for a aggregate.Root instance
	ID string

	// Root is a interface that a AggregateRoot must implement
	Root interface {
		eventSourced
		eventProducer

		EventApplier

		// ID returns the aggregateID
		AggregateID() ID
	}

	// EventApplier is a interface so that a Changed event can be applied
	EventApplier interface {
		// Apply updates the state of the aggregateRoot based on the recorded event
		// This method should only be called by the library
		Apply(event *Changed)
	}

	eventProducer interface {
		popRecordedEvents() []*Changed
	}

	eventSourced interface {
		replay(aggregate EventApplier, historyEvents []*Changed)
		recordThat(aggregate EventApplier, event *Changed)
	}
)

// GenerateID creates a new random UUID or panics
func GenerateID() ID {
	return ID(uuid.New().String())
}

// RecordChange record the given event onto the aggregate.Root by wrapping it in an aggregate.Changed
func RecordChange(aggregateRoot Root, event interface{}) error {
	aggregateID := aggregateRoot.AggregateID()
	if aggregateID == "" {
		return ErrMissingAggregateID
	}
	if event == nil {
		return ErrInvalidChangePayload
	}

	aggregateChanged := &Changed{
		aggregateID: aggregateID,
		uuid:        messaging.GenerateUUID(),
		payload:     event,
		metadata:    metadata.New(),
		createdAt:   time.Now().UTC(),
		version:     0,
	}

	aggregateRoot.recordThat(aggregateRoot, aggregateChanged)

	return nil
}
