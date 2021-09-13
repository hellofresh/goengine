package generic

import (
	"context"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/metadata"
)

// QueryExecutor is used to run a query against a inmemory event store
type QueryExecutor struct {
	store          goengine.EventStore
	streamName     goengine.StreamName
	resolver       goengine.MessagePayloadResolver
	query          goengine.Query
	queryBatchSize uint
	handlers       map[string]goengine.MessageHandler

	state  interface{}
	offset int64
}

// NewQueryExecutor returns a new QueryExecutor instance
func NewQueryExecutor(store goengine.EventStore, streamName goengine.StreamName, resolver goengine.MessagePayloadResolver, query goengine.Query, queryBatchSize uint) (*QueryExecutor, error) {
	switch {
	case store == nil:
		return nil, goengine.InvalidArgumentError("store")
	case resolver == nil:
		return nil, goengine.InvalidArgumentError("resolver")
	case query == nil:
		return nil, goengine.InvalidArgumentError("query")
	}

	handlers := query.Handlers()
	if len(handlers) == 0 {
		return nil, goengine.InvalidArgumentError("query")
	}

	return &QueryExecutor{
		store:          store,
		streamName:     streamName,
		resolver:       resolver,
		query:          query,
		queryBatchSize: queryBatchSize,
		handlers:       handlers,

		offset: 0,
		state:  nil,
	}, nil
}

// Reset sets the executor to it's initial state
func (e *QueryExecutor) Reset(ctx context.Context) {
	e.state = nil
	e.offset = 0
}

// Run executes the query and returns the final state
func (e *QueryExecutor) Run(ctx context.Context) (interface{}, error) {
	if e.offset == 0 {
		initialState, err := e.query.Init(ctx)
		if err != nil {
			return nil, err
		}

		e.state = initialState
		e.offset = 1
	}

	for {
		messages, err := e.store.Load(ctx, e.streamName, e.offset, &e.queryBatchSize, metadata.NewMatcher())
		if err != nil {
			return nil, err
		}

		var msgCount int64
		for messages.Next() {
			// Get the message
			msg, msgNumber, err := messages.Message()
			if err != nil {
				return nil, err
			}
			msgCount++
			e.offset = msgNumber

			// Resolve the payload event name
			eventName, err := e.resolver.ResolveName(msg.Payload())
			if err != nil {
				continue
			}

			// Resolve the payload handler using the event name
			handler, found := e.handlers[eventName]
			if !found {
				continue
			}

			// Execute the handler
			e.state, err = handler(ctx, e.state, msg)
			if err != nil {
				return nil, err
			}
		}

		// If the amount of messages is less than the batch size then we reached the end of the stream
		if msgCount < int64(e.queryBatchSize) {
			break
		}
	}

	return e.state, nil
}
