package sql

import "github.com/hellofresh/goengine"

// PersistenceStrategy interface describes strategy of persisting messages in the database
type PersistenceStrategy interface {
	CreateSchema(tableName string) []string
	ColumnNames() []string
	PrepareData([]goengine.Message) ([]interface{}, error)
	GenerateTableName(streamName goengine.StreamName) (string, error)
}
