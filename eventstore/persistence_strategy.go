package eventstore

import "github.com/hellofresh/goengine/messaging"

type (
	// PersistenceStrategy interface describes strategy of persisting messages in the database
	PersistenceStrategy interface {
		CreateSchema(tableName string) []string
		ColumnNames() []string
		PrepareData([]messaging.Message) ([]interface{}, error)
		GenerateTableName(streamName StreamName) (string, error)
	}
)
