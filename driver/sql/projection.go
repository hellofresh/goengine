package sql

import (
	"context"
	"database/sql"
	"github.com/hellofresh/goengine"
	"github.com/mailru/easyjson/jlexer"
	"github.com/pkg/errors"
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

	// ProjectionRawState the raw projection projectionState returned by ProjectorStorage.Acquire
	ProjectionRawState struct {
		Position        int64
		ProjectionState []byte
	}

	// ProjectionStateSerialization is an interface describing how a projection state can be initialized, serialized/encoded anf deserialized/decoded
	ProjectionStateSerialization interface {
		// init initializes the state
		Init(ctx context.Context) (interface{}, error)

		// DecodeState reconstitute the projection state based on the provided state data
		DecodeState(data []byte) (interface{}, error)

		// EncodeState encode the given object for storage
		EncodeState(obj interface{}) ([]byte, error)
	}

	// ProjectionErrorCallback is a function used to determin what action to take based on a failed projection
	ProjectionErrorCallback func(err error, notification *ProjectionNotification) ProjectionErrorAction

	// ProjectionErrorAction a type containing the action that the projector should take after an error
	ProjectionErrorAction int

	// EventStreamLoader loads a event stream based on the provided notification and state
	EventStreamLoader func(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification, position int64) (goengine.EventStream, error)

	// ProjectorStorage is an interface for handling the projection storage
	ProjectorStorage interface {
		// Acquire this function is used to acquire the projection and it's projectionState
		// A projection can only be acquired once and must be released using the returned func
		Acquire(ctx context.Context, conn *sql.Conn, notification *ProjectionNotification) (ProjectorTransaction, int64, error)
	}

	// AggregateProjectorStorage the storage interface that will persist and load the projection state
	AggregateProjectorStorage interface {
		ProjectorStorage

		LoadOutOfSync(ctx context.Context, conn Queryer) (*sql.Rows, error)

		PersistFailure(conn Execer, notification *ProjectionNotification) error
	}

	// StreamProjectorStorage the storage interface that will persist and load the projection state
	StreamProjectorStorage interface {
		ProjectorStorage

		CreateProjection(ctx context.Context, conn Execer) error
	}

	// ProjectorTransaction is a transaction type object returned by the ProjectorStorage
	ProjectorTransaction interface {
		AcquireState(ctx context.Context) (ProjectionState, error)
		CommitState(ProjectionState) error

		Close() error
	}
)

// UnmarshalJSON supports json.Unmarshaler interface
func (p *ProjectionNotification) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	p.UnmarshalEasyJSON(&r)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (p *ProjectionNotification) UnmarshalEasyJSON(in *jlexer.Lexer) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "no":
			p.No = in.Int64()
		case "aggregate_id":
			p.AggregateID = in.String()
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

// GetProjectionStateSerialization returns a ProjectionStateSerialization based on the provided projection
func GetProjectionStateSerialization(projection goengine.Projection) ProjectionStateSerialization {
	if saga, ok := projection.(ProjectionStateSerialization); ok {
		return saga
	}

	return nopProjectionStateSerialization{
		Projection: projection,
	}
}

type nopProjectionStateSerialization struct {
	goengine.Projection
}

// DecodeState reconstitute the projection state based on the provided state data
func (nopProjectionStateSerialization) DecodeState(data []byte) (interface{}, error) {
	return nil, nil
}

// EncodeState encode the given object for storage
func (nopProjectionStateSerialization) EncodeState(obj interface{}) ([]byte, error) {
	if obj == nil {
		return []byte{'{', '}'}, nil
	}

	return nil, errors.New("unexpected state provided (Did you forget to implement goengine.ProjectionSaga?)")
}
