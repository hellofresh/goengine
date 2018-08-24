package eventstore

import (
	"context"

	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

type (
	// StreamName is the name of an event stream
	StreamName string

	// EventStore an interface describing an event store
	EventStore interface {
		ReadOnlyEventStore

		// Create creates an event stream
		Create(streamName StreamName) error

		// AppendTo appends the provided messages to the stream
		AppendTo(ctx context.Context, streamName StreamName, streamEvents []messaging.Message) error
	}

	// ReadOnlyEventStore an interface describing a readonly event store
	ReadOnlyEventStore interface {
		// HasStream returns true if the stream exists
		HasStream(streamName StreamName) bool

		// Load returns a list of events based on the provided conditions
		Load(ctx context.Context, streamName StreamName, fromNumber int, count *uint, metadataMatcher metadata.Matcher) ([]messaging.Message, error)
	}

	// PayloadConverter an interface describing converting payload data
	PayloadConverter interface {
		// ConvertPayload generates unique name for the event_name
		ConvertPayload(payload interface{}) (string, []byte, error)
	}
)
