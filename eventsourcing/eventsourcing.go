package eventsourcing

import (
	"time"

	"github.com/google/uuid"
)

var (
	timeNow = time.Now

	// GenerateAggregateID creates a new random UUID or panics
	GenerateAggregateID = func() AggregateID {
		return AggregateID(uuid.New())
	}
)

type (
	// AggregateID an UUID for a AggregateRoot instance
	AggregateID [16]byte

	// AggregateRoot is a interface that a AggregateRoot must implement
	AggregateRoot interface {
		aggregateID() AggregateID

		EventRecorder

		eventProducer
	}

	// EventApplier is a interface so that a AggregateChanged event can be applied
	EventApplier interface {
		Apply(event AggregateChanged)
	}

	// EventRecorder is a interface indicating that events can be applied
	EventRecorder interface {
		EventApplier

		eventSourced
	}

	eventProducer interface {
		popRecordedEvents() []AggregateChanged
	}

	eventSourced interface {
		replay(aggregate EventApplier, historyEvents []AggregateChanged)
		recordThat(aggregate EventApplier, event AggregateChanged)
	}
)

// RecordThat record a event for an aggregate
func RecordThat(aggregate EventRecorder, event AggregateChanged) {
	aggregate.recordThat(aggregate, event)
}

// RecordAggregateChange record the given event onto the AggregateRoot by wrapping it in an AggregateChanged
func RecordAggregateChange(aggregateRoot AggregateRoot, event interface{}) {
	aggregateChanged := NewAggregateChange(aggregateRoot.aggregateID(), event)

	RecordThat(aggregateRoot, aggregateChanged)
}
