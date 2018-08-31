package inmemory

import (
	"context"
	"errors"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrEventStoreRequired occurs when a nil event store is provided
	ErrEventStoreRequired = errors.New("an EventStore may not be nil")
	// ErrPayloadRegistryRequired occurs when a nil registry is provided
	ErrPayloadRegistryRequired = errors.New("a PayloadRegistry may not be nil")
	// ErrQueryRequired occurs when a nil query is provided
	ErrQueryRequired = errors.New("a Query may not be nil")
	// ErrQueryHasNoHandlers occurs when a query is being run without any handlers
	ErrQueryHasNoHandlers = errors.New("the Query has no handlers")
	// Ensure QueryExecutor implements the eventstore.QueryExecutor interface
	_ eventstore.QueryExecutor = &QueryExecutor{}
	// queryBatchSize is the amount of event that should be fetch from the eventstore at once
	queryBatchSize uint = 100
)

// QueryExecutor is used to run a query against a inmemory event store
type QueryExecutor struct {
	store      *EventStore
	streamName eventstore.StreamName
	registry   *PayloadRegistry
	query      eventstore.Query

	state  interface{}
	offset int64
}

// NewQueryExecutor returns a new QueryExecutor instance
func NewQueryExecutor(store *EventStore, streamName eventstore.StreamName, registry *PayloadRegistry, query eventstore.Query) (*QueryExecutor, error) {
	if store == nil {
		return nil, ErrEventStoreRequired
	}
	if registry == nil {
		return nil, ErrPayloadRegistryRequired
	}
	if query == nil {
		return nil, ErrQueryRequired
	}

	return &QueryExecutor{
		store:      store,
		streamName: streamName,
		registry:   registry,
		query:      query,
		offset:     0,
		state:      nil,
	}, nil
}

// Reset sets the executor to it's initial state
func (e *QueryExecutor) Reset(ctx context.Context) {
	e.state = nil
	e.offset = 0
}

// Run executes the query and returns the final state
func (e *QueryExecutor) Run(ctx context.Context) (interface{}, error) {
	handlers := e.query.Handlers()
	if len(handlers) == 0 {
		return nil, ErrQueryHasNoHandlers
	}

	if e.offset == 0 {
		e.state = e.query.Init()
		e.offset = 1
	}

	for {
		messages, err := e.store.Load(ctx, e.streamName, e.offset, &queryBatchSize, metadata.NewMatcher())
		if err != nil {
			return nil, err
		}

		var msgCount int64
		for messages.Next() {
			msgCount++

			// Get the message
			msg, msgNumber, err := messages.Message()
			if err != nil {
				return nil, err
			}

			// Resolve the payload event name
			eventName, err := e.registry.ResolveEventName(msg.Payload())
			if err != nil {
				continue
			}

			// Resolve the payload handler using the event name
			handler, found := handlers[eventName]
			if !found {
				continue
			}

			// Execute the handler
			e.state, err = handler(e.state, msg)
			if err != nil {
				return nil, err
			}
			e.offset = msgNumber
		}

		// If the amount of messages is less than the batch size then we reached the end of the stream
		if msgCount < int64(queryBatchSize) {
			break
		}

		e.offset += msgCount
	}

	return e.state, nil
}
