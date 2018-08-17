package sql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrPayloadFactoryRequired occurs when a nil PayloadFactory is provided
	ErrPayloadFactoryRequired = errors.New("a PayloadFactory may not be nil")
	// Ensure that AggregateChangedFactory satisfies the MessageFactory interface
	_ MessageFactory = &AggregateChangedFactory{}
)

type (
	// MessageFactory reconstruct messages from the database
	MessageFactory interface {
		// CreateFromRows reconstructs the message from the provided rows
		CreateFromRows(rows *sql.Rows) ([]messaging.Message, error)
	}

	// AggregateChangedFactory reconstructs aggregate.Changed messages
	AggregateChangedFactory struct {
		payloadFactory eventstore.PayloadFactory
	}
)

// NewAggregateChangedFactory returns a new instance of an AggregateChangedFactory
func NewAggregateChangedFactory(factory eventstore.PayloadFactory) (*AggregateChangedFactory, error) {
	if factory == nil {
		return nil, ErrPayloadFactoryRequired
	}

	return &AggregateChangedFactory{
		factory,
	}, nil
}

// CreateFromRows reconstruct the aggregate.Changed messages from the sql.Rows
func (f *AggregateChangedFactory) CreateFromRows(rows *sql.Rows) ([]messaging.Message, error) {
	messages := []messaging.Message{}
	if rows == nil {
		return messages, nil
	}

	for rows.Next() {
		var (
			eventID      messaging.UUID
			eventName    string
			jsonPayload  []byte
			jsonMetadata []byte
			createdAt    time.Time
		)

		err := rows.Scan(&eventID, &eventName, &jsonPayload, &jsonMetadata, &createdAt)
		if err != nil {
			return nil, err
		}

		metadataWrapper := metadata.JSONMetadata{Metadata: metadata.New()}
		if err := json.Unmarshal(jsonMetadata, &metadataWrapper); err != nil {
			return nil, err
		}
		meta := metadataWrapper.Metadata

		payload, err := f.payloadFactory.CreatePayload(eventName, jsonPayload)
		if err != nil {
			return nil, err
		}

		aggregateID, err := aggregateIDFromMetadata(meta)
		if err != nil {
			return nil, err
		}

		aggregateVersion, err := aggregateVersionFromMetadata(meta)
		if err != nil {
			return nil, err
		}

		msg, err := aggregate.ReconstituteChange(
			aggregateID,
			eventID,
			payload,
			meta,
			createdAt,
			aggregateVersion,
		)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
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
