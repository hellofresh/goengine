package config

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	driverPostgres "github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"go.uber.org/zap"
)

// NewPostgresDB creates a new sql.DB based on the POSTGRES_DSN environment variable.
func NewPostgresDB(logger *zap.Logger) (*sql.DB, func(), error) {
	postgresDB, err := sql.Open("postgres", PostgresDSN)
	if err != nil {
		return nil, nil, err
	}

	postgresDBCloser := func() {
		if err := postgresDB.Close(); err != nil {
			logger.With(zap.Error(err)).Warn("postgresDB.Close return an error")
		}
	}

	return postgresDB, postgresDBCloser, nil
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
	eventStoreStreamTable, err := manager.PersistenceStrategy().GenerateTableName(EventStoreStreamName)
	if err != nil {
		return err
	}

	// Create the event store
	eventStore, err := manager.NewEventStore()
	if err != nil {
		return err
	}
	err = eventStore.Create(context.Background(), EventStoreStreamName)
	if err != nil && err != driverPostgres.ErrTableAlreadyExists {
		return err
	}

	// Create projection tables, triggers etc.
	var exists bool
	/* #nosec */
	err = postgresDB.QueryRow(fmt.Sprintf(
		"SELECT to_regclass(%s) IS NOT NULL",
		driverPostgres.QuoteString(StreamProjectionsTable),
	)).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		queries := postgres.StreamProjectorCreateSchema(StreamProjectionsTable, EventStoreStreamName, eventStoreStreamTable)
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
		driverPostgres.QuoteString(AggregateProjectionAccountReportTable),
	)).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		queries := append(
			[]string{`CREATE TABLE account_averages (accountID UUID NOT NULL PRIMARY KEY, debit BIGINT NOT NULL, credit BIGINT NOT NULL)`},
			postgres.AggregateProjectorCreateSchema(AggregateProjectionAccountReportTable, EventStoreStreamName, eventStoreStreamTable)...,
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
