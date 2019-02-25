package config

import (
	"database/sql"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/example/bank/domain"
	"github.com/hellofresh/goengine/extension/pq"
	"github.com/hellofresh/goengine/strategy/json"
	"github.com/hellofresh/goengine/strategy/json/sql/postgres"
)

func NewGoEngineManager(postgresDB *sql.DB, logger goengine.Logger) (*postgres.SingleStreamManager, error) {
	manager, err := postgres.NewSingleStreamManager(postgresDB, logger)
	if err != nil {
		return nil, err
	}

	// Register your events so that can be properly loaded from the event store
	if err := manager.RegisterPayloads(map[string]json.PayloadInitiator{
		domain.BankAccountOpenedName: func() interface{} {
			return domain.BankAccountOpened{}
		},
		domain.BankAccountCreditedName: func() interface{} {
			return domain.BankAccountCredited{}
		},
		domain.BankAccountDebitedName: func() interface{} {
			return domain.BankAccountDebited{}
		},
	}); err != nil {
		return nil, err
	}

	return manager, nil
}

func NewGoEngineListener(logger goengine.Logger) (*pq.Listener, error) {
	return pq.NewListener(
		PostgresDSN,
		string(EventStoreStreamName),
		time.Millisecond,
		time.Second,
		logger,
	)
}
