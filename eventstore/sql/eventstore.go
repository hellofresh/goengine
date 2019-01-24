package sql

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/metadata"
)

// ReadOnlyEventStore an interface describing a readonly event store that supports providing a SQL conn
type ReadOnlyEventStore interface {
	// LoadWithConnection returns a eventstream based on the provided constraints using the provided sql.Conn
	LoadWithConnection(ctx context.Context, conn *sql.Conn, streamName eventstore.StreamName, fromNumber int64, count *uint, metadataMatcher metadata.Matcher) (eventstore.EventStream, error)
}
