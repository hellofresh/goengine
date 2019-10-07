package sql

import (
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/metadata"
)

// PersistenceStrategy interface describes strategy of persisting messages in the database
type PersistenceStrategy interface {
	CreateSchema(tableName string) []string
	EventColumnNames() []string
	ColumnNames() []string
	PrepareData([]goengine.Message) ([]interface{}, error)
	PrepareSearch(metadata.Matcher) ([]byte, []interface{})
	GenerateTableName(streamName goengine.StreamName) (string, error)
}
