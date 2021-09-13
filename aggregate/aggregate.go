package aggregate

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/metadata"
)

// ErrInvalidID occurs when a string is not a valid ID
var ErrInvalidID = errors.New("goengine: an aggregate.ID must be a valid UUID")

type (
	// ID an UUID for a aggregate.Root instance
	ID string

	// Root is a interface that a AggregateRoot must implement
	Root interface {
		eventSourced
		eventProducer

		EventApplier

		// AggregateID returns the aggregateID
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
		replay(aggregate EventApplier, historyEvents goengine.EventStream) error
		recordThat(aggregate EventApplier, event *Changed)
	}
)

// GenerateID creates a new random UUID or panics
func GenerateID() ID {
	return ID(uuid.New().String())
}

// IDFromString creates a ID from a string
func IDFromString(str string) (ID, error) {
	id, err := uuid.Parse(str)
	if err != nil {
		return "", ErrInvalidID
	}

	return ID(id.String()), nil
}

// RecordChange record the given event onto the aggregate.Root by wrapping it in an aggregate.Changed
func RecordChange(aggregateRoot Root, event interface{}) error {
	aggregateID := aggregateRoot.AggregateID()
	switch {
	case aggregateID == "":
		return ErrMissingAggregateID
	case event == nil:
		return ErrInvalidChangePayload
	}

	aggregateChanged := &Changed{
		aggregateID: aggregateID,
		uuid:        goengine.GenerateUUID(),
		payload:     event,
		metadata:    metadata.New(),
		createdAt:   time.Now().UTC(),
		version:     0,
	}

	aggregateRoot.recordThat(aggregateRoot, aggregateChanged)

	return nil
}
