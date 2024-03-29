package sql

import (
	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/metadata"
)

// PersistenceStrategy interface describes strategy of persisting messages in the database
type PersistenceStrategy interface {
	CreateSchema(tableName string) []string
	// EventColumnNames represent the event store columns selected from the event stream table. Used by PrepareSearch
	EventColumnNames() []string
	// InsertColumnNames represent the ordered event store columns that are used to insert data into the event stream.
	InsertColumnNames() []string
	PrepareData([]goengine.Message) ([]interface{}, error)
	PrepareSearch(metadata.Matcher) ([]byte, []interface{})
	GenerateTableName(streamName goengine.StreamName) (string, error)
}
