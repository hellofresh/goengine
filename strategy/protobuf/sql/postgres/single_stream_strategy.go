package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/strategy/json/internal"
)

var (
	// Ensure SingleStreamStrategy implements strategy.PersistenceStrategy
	_ sql.PersistenceStrategy = &SingleStreamStrategy{}

	tableNameInvalidCharRegex = regexp.MustCompile("[^a-z0-9_]+")
)

// SingleStreamStrategy struct represents eventstore with single stream
type SingleStreamStrategy struct {
	converter goengine.MessagePayloadConverter
}

// NewSingleStreamStrategy is the constructor postgres for PersistenceStrategy interface
func NewSingleStreamStrategy(converter goengine.MessagePayloadConverter) (sql.PersistenceStrategy, error) {
	if converter == nil {
		return nil, goengine.InvalidArgumentError("converter")
	}

	return &SingleStreamStrategy{converter: converter}, nil
}

// CreateSchema returns a valid set of SQL statements to create the event store tables and indexes
func (s *SingleStreamStrategy) CreateSchema(tableName string) []string {
	tableName = postgres.QuoteIdentifier(tableName)

	statements := make([]string, 3)
	statements[0] = fmt.Sprintf(
		`CREATE TABLE %s (
    no BIGSERIAL,
    event_id UUID NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    payload JSON NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (no),
    CONSTRAINT aggregate_version_not_null CHECK ((metadata->>'_aggregate_version') IS NOT NULL),
    CONSTRAINT aggregate_type_not_null CHECK ((metadata->>'_aggregate_type') IS NOT NULL),
    CONSTRAINT aggregate_id_not_null CHECK ((metadata->>'_aggregate_id') IS NOT NULL),
    UNIQUE (event_id)
);`,
		tableName,
	)
	statements[1] = fmt.Sprintf(
		`CREATE UNIQUE INDEX ON %s
((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), (metadata->>'_aggregate_version'));`,
		tableName,
	)
	statements[2] = fmt.Sprintf(
		`CREATE INDEX ON %s
((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), no);`,
		tableName,
	)
	return statements
}

// ColumnNames returns the columns that need to be inserted into the table in the correct order
func (s *SingleStreamStrategy) ColumnNames() []string {
	return []string{"event_id", "event_name", "payload", "metadata", "created_at"}
}

// PrepareData transforms a slice of messaging into a flat interface slice with the correct column order
func (s *SingleStreamStrategy) PrepareData(messages []goengine.Message) ([]interface{}, error) {
	var out = make([]interface{}, 0, len(messages)*5) // optimization for the number of columns
	for _, msg := range messages {
		payloadType, payloadData, err := s.converter.ConvertPayload(msg.Payload())
		if err != nil {
			return nil, err
		}

		meta, err := internal.MarshalJSON(msg.Metadata())
		if err != nil {
			return nil, err
		}

		out = append(out,
			msg.UUID(),
			payloadType,
			payloadData,
			meta,
			msg.CreatedAt(),
		)
	}
	return out, nil
}

// GenerateTableName returns a valid table name for postgres
func (s *SingleStreamStrategy) GenerateTableName(streamName goengine.StreamName) (string, error) {
	if len(streamName) == 0 {
		return "", goengine.InvalidArgumentError("streamName")
	}

	name := strings.ToLower(string(streamName))
	// remove not allowed symbols
	name = tableNameInvalidCharRegex.ReplaceAllString(name, "")
	// remove underscore at the end
	name = strings.TrimRight(name, "_")
	// prefix with events_
	return fmt.Sprintf("events_%s", name), nil
}
