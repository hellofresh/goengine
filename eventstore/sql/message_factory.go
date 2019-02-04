package sql

import (
	"database/sql"
	"errors"

	goengine_dev "github.com/hellofresh/goengine-dev"
)

var (
	// ErrPayloadFactoryRequired occurs when a nil PayloadFactory is provided
	ErrPayloadFactoryRequired = errors.New("a PayloadFactory may not be nil")
	// ErrRowsRequired occurs when the rows passed to MessageFactory.CreateFromRows is nil
	ErrRowsRequired = errors.New("the provided rows may not be nil")
)

// MessageFactory reconstruct messages from the database
type MessageFactory interface {
	// CreateEventStream reconstructs the message from the provided rows
	CreateEventStream(rows *sql.Rows) (goengine_dev.EventStream, error)
}
