package sql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/metadata"
)

// Ensure that AggregateChangedFactory satisfies the MessageFactory interface
var _ driverSQL.MessageFactory = &AggregateChangedFactory{}

// AggregateChangedFactory reconstructs aggregate.Changed messages
type AggregateChangedFactory struct {
	payloadFactory goengine.MessagePayloadFactory
}

// NewAggregateChangedFactory returns a new instance of an AggregateChangedFactory
func NewAggregateChangedFactory(factory goengine.MessagePayloadFactory) (*AggregateChangedFactory, error) {
	if factory == nil {
		return nil, goengine.InvalidArgumentError("factory")
	}

	return &AggregateChangedFactory{
		factory,
	}, nil
}

// CreateEventStream reconstruct the aggregate.Changed messages from the sql.Rows
func (f *AggregateChangedFactory) CreateEventStream(rows *sql.Rows) (goengine.EventStream, error) {
	if rows == nil {
		return nil, goengine.InvalidArgumentError("rows")
	}

	return &aggregateChangedEventStream{
		payloadFactory: f.payloadFactory,
		rows:           rows,
	}, nil
}

// Ensure that aggregateChangedEventStream satisfies the eventstore.EventStream interface
var _ goengine.EventStream = &aggregateChangedEventStream{}

type aggregateChangedEventStream struct {
	payloadFactory goengine.MessagePayloadFactory
	rows           *sql.Rows
}

func (a *aggregateChangedEventStream) Next() bool {
	return a.rows.Next()
}

func (a *aggregateChangedEventStream) Err() error {
	return a.rows.Err()
}

func (a *aggregateChangedEventStream) Close() error {
	return a.rows.Close()
}

func (a *aggregateChangedEventStream) Message() (goengine.Message, int64, error) {
	var (
		eventNumber  int64
		eventID      goengine.UUID
		eventName    string
		jsonPayload  []byte
		jsonMetadata []byte
		createdAt    time.Time
	)

	err := a.rows.Scan(&eventNumber, &eventID, &eventName, &jsonPayload, &jsonMetadata, &createdAt)
	if err != nil {
		return nil, 0, err
	}

	meta, err := metadata.UnmarshalJSON(jsonMetadata)
	if err != nil {
		return nil, 0, err
	}

	payload, err := a.payloadFactory.CreatePayload(eventName, jsonPayload)
	if err != nil {
		return nil, 0, err
	}

	aggregateID, err := aggregateIDFromMetadata(meta)
	if err != nil {
		return nil, 0, err
	}

	aggregateVersion, err := aggregateVersionFromMetadata(meta)
	if err != nil {
		return nil, 0, err
	}

	aggr, err := aggregate.ReconstituteChange(
		aggregateID,
		eventID,
		payload,
		meta,
		createdAt,
		aggregateVersion,
	)

	return aggr, eventNumber, err
}

func aggregateIDFromMetadata(meta metadata.Metadata) (aggregate.ID, error) {
	val := meta.Value(aggregate.IDKey)
	if val == nil {
		return "", MissingMetadataError(aggregate.IDKey)
	}

	str, ok := val.(string)
	if !ok {
		return "", &InvalidMetadataValueTypeError{key: aggregate.IDKey, value: val, expected: "string"}
	}

	return aggregate.IDFromString(str)
}

func aggregateVersionFromMetadata(meta metadata.Metadata) (uint, error) {
	val := meta.Value(aggregate.VersionKey)
	if val == nil {
		return 0, MissingMetadataError(aggregate.VersionKey)
	}

	float, ok := val.(float64)
	if !ok {
		return 0, &InvalidMetadataValueTypeError{key: aggregate.VersionKey, value: val, expected: "float64"}
	}

	if float <= 0 {
		return 0, aggregate.ErrInvalidChangeVersion
	}

	return uint(float), nil
}

// MissingMetadataError is an error indicating the requested metadata was nil.
type MissingMetadataError string

func (e MissingMetadataError) Error() string {
	return "goengine: metadata key " + string(e) + " is not set or nil"
}

// InvalidMetadataValueTypeError is an error indicating the value metadata key was an unexpected type.
type InvalidMetadataValueTypeError struct {
	key      string
	value    interface{}
	expected string
}

func (e *InvalidMetadataValueTypeError) Error() string {
	return fmt.Sprintf("goengine: metadata key %s with value %v was expected to be of type %s", e.key, e.value, e.expected)
}
