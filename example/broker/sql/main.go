package main

import (
	"context"
	"database/sql"
	"fmt"
	driverPostgres "github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/example/broker/lib"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"go.uber.org/zap"
	"time"
)

func main() {
	logger, err := zap.NewDevelopment()
	failOnErr(err)

	db, dbCloser, err := lib.NewPostgresDB(logger)
	failOnErr(err)
	defer dbCloser()

	manager, err := lib.NewGoEngineManager(db, goengineZap.Wrap(logger))
	failOnErr(err)

	failOnErr(SetupDB(manager, db, logger))
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

// SetupDB created the needed tables for the application
func SetupDB(manager *postgres.SingleStreamManager, postgresDB *sql.DB, logger *zap.Logger) error {
	// Ensure we are connected
	for i := 0; ; i++ {
		err := postgresDB.Ping()
		if err == nil {
			break
		}

		if i > 5 {
			return err
		}
		logger.With(zap.Error(err)).Warn("failed to ping db waiting to try again")
		time.Sleep(time.Second)
	}

	// Generate the event store table name
	eventStoreStreamTable, err := manager.PersistenceStrategy().GenerateTableName(lib.EventStoreStreamName)
	if err != nil {
		return err
	}

	// Create the event store
	eventStore, err := manager.NewEventStore()
	if err != nil {
		return err
	}
	err = eventStore.Create(context.Background(), lib.EventStoreStreamName)
	if err != nil && err != driverPostgres.ErrTableAlreadyExists {
		return err
	}

	// Create projection tables, triggers etc.
	var exists bool
	/* #nosec */
	err = postgresDB.QueryRow(fmt.Sprintf(
		"SELECT to_regclass(%s) IS NOT NULL",
		driverPostgres.QuoteString(lib.StreamProjectionsTable),
	)).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		queries := postgres.StreamProjectorCreateSchema(lib.StreamProjectionsTable, lib.EventStoreStreamName, eventStoreStreamTable)
		for _, query := range queries {
			_, err := postgresDB.Exec(query)
			if err != nil {
				logger.With(zap.Error(err), zap.String("query", query)).Panic("failed to execute query")
				return err
			}
		}
	}

	// Create aggregate account report projection tables
	/* #nosec */
	err = postgresDB.QueryRow(fmt.Sprintf(
		"SELECT to_regclass(%s) IS NOT NULL",
		driverPostgres.QuoteString(lib.AggregateProjectionAccountReportTable),
	)).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		queries := append(
			[]string{`CREATE TABLE account_averages (accountID UUID NOT NULL PRIMARY KEY, debit BIGINT NOT NULL, credit BIGINT NOT NULL)`},
			postgres.AggregateProjectorCreateSchema(lib.AggregateProjectionAccountReportTable, lib.EventStoreStreamName, eventStoreStreamTable)...,
		)
		for _, query := range queries {
			_, err := postgresDB.Exec(query)
			if err != nil {
				logger.With(zap.Error(err), zap.String("query", query)).Panic("failed to execute query")
				return err
			}
		}
	}

	return nil
}
