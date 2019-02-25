package main

import (
	"context"
	"os"
	"os/signal"

	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/example/bank/config"
	"github.com/hellofresh/goengine/example/bank/projection"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	failOnError(err)

	goengineLogger := goengineZap.Wrap(logger)

	db, dbCloser, err := config.NewPostgresDB(logger)
	failOnError(err)
	defer dbCloser()

	manager, err := config.NewGoEngineManager(db, goengineLogger)
	failOnError(err)
	failOnError(config.SetupDB(manager, db, logger))

	listener, err := config.NewGoEngineListener(goengineLogger)
	failOnError(err)

	projector, err := manager.NewStreamProjector(
		config.StreamProjectionsTable,
		projection.NewBankReportProjection(db),
		func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
	)
	failOnError(err)

	run(projector, listener, logger)
}

func run(projector *postgres.StreamProjector, listener driverSQL.Listener, logger *zap.Logger) {
	ctx, cancelCtx := context.WithCancel(context.Background())

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		cancelCtx()
	}()

	if err := projector.RunAndListen(ctx, listener); err != nil && err != context.Canceled {
		logger.With(zap.Error(err)).Error("projector.RunAndListen return an error")
	}
}

func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}
