package inmemory

import (
	"errors"

	"github.com/hellofresh/goengine"
)

var (
	// ErrMessageNumberCountMismatch occurs when the provided messages and numbers do not have the same length
	ErrMessageNumberCountMismatch = errors.New("provided messages and messageNumbers do not match")
	// ErrEventStreamClosed occurs when an eventstream is closed
	ErrEventStreamClosed = errors.New("no more messages")
	// ErrEventStreamNotStarted occurs when an eventstream Message or MessageNumber is called before Next
	ErrEventStreamNotStarted = errors.New("eventStream Message called without calling Next")
	// Ensure that EventStream satisfies the eventstore.EventStream interface
	_ goengine.EventStream = &EventStream{}
)

// EventStream an inmemory eventstore.EventStream implementation
type EventStream struct {
	messages       []goengine.Message
	messageNumbers []int64

	index        int
	messageCount int
	closed       bool
}

// NewEventStream return a new EventStream containing the given messages
func NewEventStream(messages []goengine.Message, messageNumbers []int64) (*EventStream, error) {
	messageCount := len(messages)
	if len(messageNumbers) != messageCount {
		return nil, ErrMessageNumberCountMismatch
	}

	return &EventStream{
		messages:       messages,
		messageNumbers: messageNumbers,

		index:        -1,
		messageCount: len(messages),
	}, nil
}

// Next prepares the next result for reading.
func (e *EventStream) Next() bool {
	if e.closed {
		return false
	}

	e.index++
	if e.index >= e.messageCount {
		_ = e.Close()
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
	e.messageNumbers = nil

	return nil
}

// Message returns the current message in the EventStream.
func (e *EventStream) Message() (goengine.Message, int64, error) {
	if e.closed {
		return nil, 0, ErrEventStreamClosed
	}

	if e.index == -1 {
		return nil, 0, ErrEventStreamNotStarted
	}

	return e.messages[e.index], e.messageNumbers[e.index], nil
}
