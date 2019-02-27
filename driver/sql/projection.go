package sql

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
)

type (
	// ProjectionNotification is a representation of the data provided by database notify
	ProjectionNotification struct {
		No          int64  `json:"no"`
		AggregateID string `json:"aggregate_id"`
	}

	// ProjectionTrigger triggers the notification for processing
	ProjectionTrigger func(ctx context.Context, notification *ProjectionNotification) error

	// ProjectionState is a projection projectionState
	ProjectionState struct {
		Position        int64
		ProjectionState interface{}
	}

	// ProjectionRawState the raw projection projectionState returned by ProjectionStorage.Acquire
	ProjectionRawState struct {
		Position        int64
		ProjectionState []byte
	}

	// ProjectionStateInitializer is a func to initialize a ProjectionState.ProjectionState
	ProjectionStateInitializer func(ctx context.Context) (interface{}, error)

	// ProjectionStateEncoder is a func to marshal the ProjectionState.ProjectionState
	ProjectionStateEncoder func(interface{}) ([]byte, error)

	// ProjectionStateDecoder is a func to unmarshal the ProjectionRawState.ProjectionState
	ProjectionStateDecoder func(data []byte) (interface{}, error)

	// ProjectionStorage is an interface for handling the projection storage
	ProjectionStorage interface {
		// PersistState persists the state of the projection
		PersistState(conn Execer, notification *ProjectionNotification, state ProjectionState) error

		// Acquire this function is used to acquire the projection and it's projectionState
		// A projection can only be acquired once and must be released using the returned func
		Acquire(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification) (func(), *ProjectionRawState, error)
	}

	// ProjectionErrorCallback is a function used to determin what action to take based on a failed projection
	ProjectionErrorCallback func(err error, notification *ProjectionNotification) ProjectionErrorAction

	// ProjectionErrorAction a type containing the action that the projector should take after an error
	ProjectionErrorAction int

	// EventStreamLoader loads a event stream based on the provided notification and state
	EventStreamLoader func(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification, state ProjectionState) (goengine.EventStream, error)
)
