package aggregate

import (
	"errors"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrMissingAggregateID occurs when no or an invalid aggregate.ID was provided
	ErrMissingAggregateID = errors.New("goengine: no or empty aggregate ID was provided")
	// ErrMissingChangeUUID occurs when no or an invalid message.UUID was provided
	ErrMissingChangeUUID = errors.New("goengine: no or empty message UUID was provided")
	// ErrInvalidChangeVersion occurs since a version cannot be zero
	ErrInvalidChangeVersion = errors.New("goengine: a changed event must have a version number greater than zero")
	// ErrInvalidChangePayload occurs when no payload is provided
	ErrInvalidChangePayload = errors.New("goengine: a changed event must have a payload that is not nil")
)

// Changed is a message indicating that a aggregate was changed
type Changed struct {
	uuid        goengine.UUID
	aggregateID ID
	payload     interface{}
	metadata    metadata.Metadata
	createdAt   time.Time
	version     uint
}

// ReconstituteChange recreates a previous aggregate Changed message based on the provided data
func ReconstituteChange(
	aggregateID ID,
	uuid goengine.UUID,
	payload interface{},
	metadata metadata.Metadata,
	createdAt time.Time,
	version uint,
) (*Changed, error) {
	switch {
	case aggregateID == "":
		return nil, ErrMissingAggregateID
	case goengine.IsUUIDEmpty(uuid):
		return nil, ErrMissingChangeUUID
	case payload == nil:
		return nil, ErrInvalidChangePayload
	case version == 0:
		return nil, ErrInvalidChangeVersion
	}

	return &Changed{
		aggregateID: aggregateID,
		uuid:        uuid,
		payload:     payload,
		metadata:    metadata,
		createdAt:   createdAt,
		version:     version,
	}, nil
}

// UUID returns the unique message identifier
func (a *Changed) UUID() goengine.UUID {
	return a.uuid
}

// AggregateID returns the aggregate ID
func (a *Changed) AggregateID() ID {
	return a.aggregateID
}

// CreatedAt returns the created time
func (a *Changed) CreatedAt() time.Time {
	return a.createdAt
}

// Version return the version of aggregate this change represents
func (a *Changed) Version() uint {
	return a.version
}

// Payload returns the payload of the change
// This is the actual domain event
func (a *Changed) Payload() interface{} {
	return a.payload
}

// Metadata return the change metadata
func (a *Changed) Metadata() metadata.Metadata {
	return a.metadata
}

// WithMetadata Returns new instance of the change with key and value added to metadata
func (a Changed) WithMetadata(key string, value interface{}) goengine.Message {
	a.metadata = metadata.WithValue(a.metadata, key, value)

	return &a
}

func (a Changed) withVersion(version uint) *Changed {
	a.version = version

	return &a
}
