package eventstore

import "github.com/hellofresh/goengine/messaging"

// PersistenceStrategy interface describes strategy of persisting messages in the database
type PersistenceStrategy interface {
	CreateSchema(tableName string) []string
	ColumnNames() []string
	PrepareData([]messaging.Message) ([]interface{}, error)
	GenerateTableName(streamName StreamName) (string, error)
}
