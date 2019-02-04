package sql

import (
	"context"
	"database/sql"

	goengine_dev "github.com/hellofresh/goengine-dev"

	"github.com/hellofresh/goengine/metadata"
)

// ReadOnlyEventStore an interface describing a readonly event store that supports providing a SQL conn
type ReadOnlyEventStore interface {
	// LoadWithConnection returns a eventstream based on the provided constraints using the provided sql.Conn
	LoadWithConnection(ctx context.Context, conn *sql.Conn, streamName goengine_dev.StreamName, fromNumber int64, count *uint, metadataMatcher metadata.Matcher) (goengine_dev.EventStream, error)
}
