package eventstore

import (
	"context"

	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

type (
	// StreamName is the name of an event stream
	StreamName string

	// EventStream is the result of an event store query. Its cursor starts before the first row
	// of the result set. Use Next to advance through the results:
	EventStream interface {
		// Next prepares the next result for reading.
		// It returns true on success, or false if there is no next result row or an error happened while preparing it.
		// Err should be consulted to distinguish between the two cases.
		Next() bool

		// Err returns the error, if any, that was encountered during iteration.
		Err() error

		// Close closes the EventStream, preventing further enumeration. If Next is called
		// and returns false and there are no further result sets,
		// result of Err. Close is idempotent and does not affect the result of Err.
		Close() error

		// Message returns the current message in the EventStream.
		Message() (messaging.Message, error)
	}

	// EventStore an interface describing an event store
	EventStore interface {
		ReadOnlyEventStore

		// Create creates an event stream
		Create(ctx context.Context, streamName StreamName) error

		// AppendTo appends the provided messages to the stream
		AppendTo(ctx context.Context, streamName StreamName, streamEvents []messaging.Message) error
	}

	// ReadOnlyEventStore an interface describing a readonly event store
	ReadOnlyEventStore interface {
		// HasStream returns true if the stream exists
		HasStream(ctx context.Context, streamName StreamName) bool

		// Load returns a list of events based on the provided conditions
		Load(ctx context.Context, streamName StreamName, fromNumber int, count *uint, metadataMatcher metadata.Matcher) (EventStream, error)
	}
)

// ReadEventStream reads the entire event stream and returns it's content as a slice.
// The main purpose of the function is for testing and debugging.
func ReadEventStream(stream EventStream) ([]messaging.Message, error) {
	var messages []messaging.Message
	for stream.Next() {
		msg, err := stream.Message()
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	if err := stream.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}
