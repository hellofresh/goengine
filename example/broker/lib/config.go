package lib

import (
	"database/sql"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/extension/pq"
	"github.com/hellofresh/goengine/strategy/json"
	"github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

const (
	// EventStoreStreamName is the name of the event stream used for you bank account
	EventStoreStreamName goengine.StreamName = "bank"
	// StreamProjectionsTable is the name of the table used to store the state of the stream projections
	StreamProjectionsTable = "bank_projection"
	// AggregateProjectionAccountReportTable is the name of the table used to store the states of the account average aggregate projection
	AggregateProjectionAccountReportTable = "account_averages_projection"
)


var (
	// PostgresDSN the Postgres DSN used to connect to progres
	PostgresDSN string
	// AMQPDSN the AQMP DSN used to connect to RabbitMQ
	AMQPDSN string
)

func init() {
	PostgresDSN = os.Getenv("POSTGRES_DSN")
	if strings.TrimSpace(PostgresDSN) == "" {
		panic("expected POSTGRES_DSN to be set and not empty")
	}

	AMQPDSN = os.Getenv("AMQP_DSN")
	if strings.TrimSpace(AMQPDSN) == "" {
		panic("expected AMQP_DSN to be set and not empty")
	}
}

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

// NewGoEngineManager return a configured SingleStreamManager for the application
func NewGoEngineManager(postgresDB *sql.DB, logger goengine.Logger) (*postgres.SingleStreamManager, error) {
	manager, err := postgres.NewSingleStreamManager(postgresDB, logger, nil)
	if err != nil {
		return nil, err
	}

	// Register your events so that can be properly loaded from the event store
	if err := manager.RegisterPayloads(map[string]json.PayloadInitiator{
		BankAccountOpenedEventName: func() interface{} {
			return BankAccountOpened{}
		},
		BankAccountCreditedEventName: func() interface{} {
			return BankAccountCredited{}
		},
		BankAccountDebitedEventName: func() interface{} {
			return BankAccountDebited{}
		},
	}); err != nil {
		return nil, err
	}

	return manager, nil
}

// NewGoEngineListener returns a new goengine db listener
func NewGoEngineListener(logger goengine.Logger) (*pq.Listener, error) {
	return pq.NewListener(
		PostgresDSN,
		string(EventStoreStreamName),
		time.Millisecond,
		time.Second,
		logger,
		nil,
	)
}
