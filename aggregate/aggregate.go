package aggregate

import (
	"errors"
	"time"

	"github.com/google/uuid"
	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/metadata"
)

// ErrInvalidID occurs when a string is not a valid ID
var ErrInvalidID = errors.New("an aggregate.ID must be a valid UUID")

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
	if aggregateID == "" {
		return ErrMissingAggregateID
	}
	if event == nil {
		return ErrInvalidChangePayload
	}

	aggregateChanged := &Changed{
		aggregateID: aggregateID,
		uuid:        goengine_dev.GenerateUUID(),
		payload:     event,
		metadata:    metadata.New(),
		createdAt:   time.Now().UTC(),
		version:     0,
	}

	aggregateRoot.recordThat(aggregateRoot, aggregateChanged)

	return nil
}
