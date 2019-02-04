package eventstore

import (
	"context"
	"errors"

	goengine_dev "github.com/hellofresh/goengine-dev"

	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrEventStoreRequired occurs when a nil event store is provided
	ErrEventStoreRequired = errors.New("an EventStore may not be nil")
	// ErrPayloadResolverRequired occurs when a nil resolver is provided
	ErrPayloadResolverRequired = errors.New("an MessagePayloadResolver may not be nil")
	// ErrQueryRequired occurs when a nil query is provided
	ErrQueryRequired = errors.New("a Query may not be nil")
	// ErrQueryHasNoHandlers occurs when a query is being run without any handlers
	ErrQueryHasNoHandlers = errors.New("the Query has no handlers")
)

// QueryExecutor is used to run a query against a inmemory event store
type QueryExecutor struct {
	store          goengine_dev.EventStore
	streamName     goengine_dev.StreamName
	resolver       goengine_dev.MessagePayloadResolver
	query          goengine_dev.Query
	queryBatchSize uint

	state  interface{}
	offset int64
}

// NewQueryExecutor returns a new QueryExecutor instance
func NewQueryExecutor(store goengine_dev.EventStore, streamName goengine_dev.StreamName, resolver goengine_dev.MessagePayloadResolver, query goengine_dev.Query, queryBatchSize uint) (*QueryExecutor, error) {
	if store == nil {
		return nil, ErrEventStoreRequired
	}
	if resolver == nil {
		return nil, ErrPayloadResolverRequired
	}
	if query == nil {
		return nil, ErrQueryRequired
	}

	return &QueryExecutor{
		store:          store,
		streamName:     streamName,
		resolver:       resolver,
		query:          query,
		queryBatchSize: queryBatchSize,

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
	handlers := e.query.Handlers()
	if len(handlers) == 0 {
		return nil, ErrQueryHasNoHandlers
	}

	if e.offset == 0 {
		e.state = e.query.Init()
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
			handler, found := handlers[eventName]
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
