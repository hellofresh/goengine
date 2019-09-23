package main

import (
	"context"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/example/broker/lib"
	"github.com/hellofresh/goengine/extension/amqp"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	logger, err := zap.NewDevelopment()
	failOnErr(err)

	db, dbCloser, err := lib.NewPostgresDB(logger)
	failOnErr(err)
	defer dbCloser()

	manager, err := lib.NewGoEngineManager(db, goengineZap.Wrap(logger))
	failOnErr(err)

	goengineLogger := goengineZap.Wrap(logger)

	projection := NewAccountAverageProjection(db)

	projector, err := manager.NewAggregateProjector(
		lib.EventStoreStreamName,
		lib.BankAccountTypeName,
		lib.AggregateProjectionAccountReportTable,
		projection,
		func(error, *sql.ProjectionNotification) sql.ProjectionErrorAction {
			return sql.ProjectionFail
		},
		true,
		0,
	)
	failOnErr(err)

	consumer, err := amqp.NewNotificationConsumer(
		lib.AMQPDSN,
		"goengine_example",
		goengineLogger,
	)
	failOnErr(err)

	// TODO os.signal
	failOnErr(consumer.Listen(ctx, projector))
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
