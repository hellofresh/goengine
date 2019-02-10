package postgres

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
)

type (
	// ConjoinedEventStore a in postgres event store implementation which includes projection logic.
	ConjoinedEventStore struct {
		*EventStore

		resolver goengine.MessagePayloadResolver
		handlers map[string]ConjoinedMessageHandler
	}

	// ConjoinedMessageHandler is a message handler called by the ConjoinedEventStore when a message is appended
	ConjoinedMessageHandler func(ctx context.Context, tx *sql.Tx, message goengine.Message) error
)

// NewConjoinedEventStore return a new postgres.ConjoinedEventStore
func NewConjoinedEventStore(
	eventstore *EventStore,
	resolver goengine.MessagePayloadResolver,
	handlers map[string]ConjoinedMessageHandler,
) (*ConjoinedEventStore, error) {

	return &ConjoinedEventStore{
		EventStore: eventstore,
		resolver:   resolver,
		handlers:   handlers,
	}, nil
}

// AppendTo batch inserts Messages into the event stream table
func (e *ConjoinedEventStore) AppendTo(ctx context.Context, streamName goengine.StreamName, streamEvents []goengine.Message) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			e.logger.
				WithError(err).
				Error("could not rollback transaction")
		}
	}()

	// Append the messages to the eventstore
	if err := e.AppendToWithExecer(ctx, tx, streamName, streamEvents); err != nil {
		return err
	}

	// Trigger all needed projections
	for _, msg := range streamEvents {
		// Resolve the payload event name
		eventName, err := e.resolver.ResolveName(msg.Payload())
		if err != nil {
			e.logger.
				WithField("payload", msg.Payload()).
				Warn("skipping event: unable to resolve payload name")
			continue
		}

		// Resolve the payload handler using the event name
		handler, found := e.handlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		if err = handler(ctx, tx, msg); err != nil {
			return err
		}
	}

	return tx.Commit()
}
