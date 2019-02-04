package eventstore

import goengine_dev "github.com/hellofresh/goengine-dev"

// PersistenceStrategy interface describes strategy of persisting messages in the database
type PersistenceStrategy interface {
	CreateSchema(tableName string) []string
	ColumnNames() []string
	PrepareData([]goengine_dev.Message) ([]interface{}, error)
	GenerateTableName(streamName StreamName) (string, error)
}
