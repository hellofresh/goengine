package main

import (
	"context"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/example/broker/lib"
	"github.com/hellofresh/goengine/extension/amqp"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	failOnErr(err)

	goengineLogger := goengineZap.Wrap(logger)

	listener, err := lib.NewGoEngineListener(goengineLogger)
	failOnErr(err)

	db, dbCloser, err := lib.NewPostgresDB(logger)
	failOnErr(err)
	defer dbCloser()

	manager, err := lib.NewGoEngineManager(db, goengineZap.Wrap(logger))
	failOnErr(err)

	publisher, err := amqp.NewNotificationPublisher(
		lib.AMQPDSN,
		"goengine_example",
		goengineLogger,
	)
	failOnErr(err)

	ctx := context.Background()

	eventStoreTableName, err := manager.PersistenceStrategy().GenerateTableName(lib.EventStoreStreamName)
	failOnErr(err)

	projection := lib.NewAccountAverageProjection(db)
	projectorStorage, err := postgres.NewAdvisoryLockAggregateProjectionStorage(
		eventStoreTableName,
		lib.AggregateProjectionAccountReportTable,
		sql.GetProjectionStateSerialization(projection),
		true,
		goengineLogger,
	)

	publish, err := sql.NewNotificationAggregateSyncHandler(publisher.Publish, db, projectorStorage, publisher.Publish, goengineLogger)
	failOnErr(err)

	// TODO os.signal
	failOnErr(listener.Listen(ctx, publish))
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
