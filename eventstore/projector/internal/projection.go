package internal

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/projector"
)

type (
	// Trigger triggers the notification for processing
	Trigger func(ctx context.Context, notification *projector.Notification) error

	// AcquiredState the raw projection projectionState returned by Acquire
	AcquiredState struct {
		Position        int64
		ProjectionState []byte
	}

	// UnmarshalAcquiredState is a func to unmarshal the AcquiredState.ProjectionState
	UnmarshalAcquiredState func(data []byte) (interface{}, error)

	// State is a projection projectionState
	State struct {
		Position        int64
		ProjectionState interface{}
	}

	// EventStreamLoader loads a event stream based on the provided notification and state
	EventStreamLoader func(ctx context.Context, conn *sql.Conn, notification *projector.Notification, state State) (eventstore.EventStream, error)

	// Storage is an interface for handling the projection storage
	Storage interface {
		// PersistState persists the state of the projection
		PersistState(conn *sql.Conn, notification *projector.Notification, state State) error

		// Acquire this function is used to acquire the projection and it's projectionState
		// A projection can only be acquired once and must be released using the returned func
		Acquire(ctx context.Context, conn *sql.Conn, notification *projector.Notification) (func(), *AcquiredState, error)
	}
)
