package inmemory

import (
	"errors"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/messaging"
)

// Ensure that EventStream satisfies the eventstore.EventStream interface
var _ eventstore.EventStream = &EventStream{}

// EventStream a inmemory eventstore.EventStream implementation
type EventStream struct {
	messages []messaging.Message

	index        int
	messageCount int
	closed       bool
}

// NewEventStream return a new EventStream containing the given messages
func NewEventStream(messages []messaging.Message) *EventStream {
	return &EventStream{
		index:        -1,
		messages:     messages,
		messageCount: len(messages),
	}
}

// Next prepares the next result for reading.
func (e *EventStream) Next() bool {
	if e.closed {
		return false
	}

	e.index++
	if e.index >= e.messageCount {
		e.Close()
		return false
	}

	return true
}

// Err returns the error, if any, that was encountered during iteration.
func (e *EventStream) Err() error {
	return nil
}

// Close closes the EventStream, preventing further enumeration.
func (e *EventStream) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	e.messages = nil

	return nil
}

// Message returns the current message in the EventStream.
func (e *EventStream) Message() (messaging.Message, error) {
	if e.closed {
		return nil, errors.New("no more messages")
	}

	if e.index == -1 {
		return nil, errors.New("eventStream Message called without calling Next")
	}

	return e.messages[e.index], nil
}
