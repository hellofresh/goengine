package sql

import (
	"database/sql"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/eventstore"
)

// Ensure that AggregateChangedFactory satisfies the MessageFactory interface
var _ MessageFactory = &AggregateChangedFactory{}

// AggregateChangedFactory reconstructs aggregate.Changed messages
type AggregateChangedFactory struct {
	payloadFactory eventstore.PayloadFactory
}

// NewAggregateChangedFactory returns a new instance of an AggregateChangedFactory
func NewAggregateChangedFactory(factory eventstore.PayloadFactory) (*AggregateChangedFactory, error) {
	if factory == nil {
		return nil, ErrPayloadFactoryRequired
	}

	return &AggregateChangedFactory{
		factory,
	}, nil
}

// CreateEventStream reconstruct the aggregate.Changed messages from the sql.Rows
func (f *AggregateChangedFactory) CreateEventStream(rows *sql.Rows) (goengine_dev.EventStream, error) {
	if rows == nil {
		return nil, ErrRowsRequired
	}

	return newAggregateChangedEventStream(f.payloadFactory, rows)
}
