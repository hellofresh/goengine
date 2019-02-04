package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/metadata"
)

// Ensure that aggregateChangedEventStream satisfies the eventstore.EventStream interface
var _ goengine_dev.EventStream = &aggregateChangedEventStream{}

type aggregateChangedEventStream struct {
	payloadFactory eventstore.MessagePayloadFactory
	rows           *sql.Rows
}

func newAggregateChangedEventStream(factory eventstore.MessagePayloadFactory, rows *sql.Rows) (*aggregateChangedEventStream, error) {
	if factory == nil {
		return nil, ErrPayloadFactoryRequired
	}

	return &aggregateChangedEventStream{
		payloadFactory: factory,
		rows:           rows,
	}, nil
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

func (a *aggregateChangedEventStream) Message() (goengine_dev.Message, int64, error) {
	var (
		eventNumber  int64
		eventID      goengine_dev.UUID
		eventName    string
		jsonPayload  []byte
		jsonMetadata []byte
		createdAt    time.Time
	)

	err := a.rows.Scan(&eventNumber, &eventID, &eventName, &jsonPayload, &jsonMetadata, &createdAt)
	if err != nil {
		return nil, 0, err
	}

	metadataWrapper := metadata.JSONMetadata{Metadata: metadata.New()}
	if err := json.Unmarshal(jsonMetadata, &metadataWrapper); err != nil {
		return nil, 0, err
	}
	meta := metadataWrapper.Metadata

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
		return "", &MissingMetadataError{key: aggregate.IDKey}
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
		return 0, &MissingMetadataError{key: aggregate.VersionKey}
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
type MissingMetadataError struct {
	key string
}

func (e *MissingMetadataError) Error() string {
	return fmt.Sprintf("metadata key %s is not set or nil", e.key)
}

// InvalidMetadataValueTypeError is an error indicating the value metadata key was an unexpected type.
type InvalidMetadataValueTypeError struct {
	key      string
	value    interface{}
	expected string
}

func (e *InvalidMetadataValueTypeError) Error() string {
	return fmt.Sprintf("metadata key %s with value %v was expected to be of type %s", e.key, e.value, e.expected)
}
