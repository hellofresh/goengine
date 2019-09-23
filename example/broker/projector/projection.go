package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/hellofresh/goengine/example/broker/lib"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
)

var _ goengine.ProjectionSaga = &AccountAverageProjection{}

type (
	// AccountAverageProjection the account average projection
	AccountAverageProjection struct {
		db *sql.DB
	}

	// AccountAverageState the account average state used by the AccountAverageProjection to store data between events
	AccountAverageState struct {
		CreditCount  uint
		CreditAmount uint
		DebitCount   uint
		DebitAmount  uint
	}
)

// DecodeState decodes the state information stored in the eventstore
func (p *AccountAverageProjection) DecodeState(data []byte) (interface{}, error) {
	var state AccountAverageState
	err := json.Unmarshal(data, &state)
	return state, err
}

// EncodeState encodes the state information that will be stored in the eventstore
func (p *AccountAverageProjection) EncodeState(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// NewAccountAverageProjection returns a new AccountAverageProjection
func NewAccountAverageProjection(db *sql.DB) *AccountAverageProjection {
	return &AccountAverageProjection{db: db}
}

// Name return the name of the projection
func (*AccountAverageProjection) Name() string {
	return "account_averages"
}

// FromStream returns the name of the stream that this projection uses
func (*AccountAverageProjection) FromStream() goengine.StreamName {
	return lib.EventStoreStreamName
}

// Init initialized the state of the projection in this case the AccountAverageState struct
func (p *AccountAverageProjection) Init(ctx context.Context) (interface{}, error) {
	return AccountAverageState{}, nil
}

// Handlers return the handlers for the events we want to project
func (p *AccountAverageProjection) Handlers() map[string]goengine.MessageHandler {
	return map[string]goengine.MessageHandler{
		lib.BankAccountOpenedEventName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			_, err := p.db.ExecContext(ctx,
				"INSERT INTO account_averages (accountID, credit, debit) VALUES ($1, 0, 0)",
				message.Payload().(lib.BankAccountOpened).AccountID,
			)
			return state, err
		},
		lib.BankAccountCreditedEventName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			changedMsg := message.(*aggregate.Changed)
			event := message.Payload().(lib.BankAccountCredited)

			accountState := state.(AccountAverageState)
			accountState.CreditAmount += event.Amount
			accountState.CreditCount++

			_, err := p.db.ExecContext(ctx,
				"UPDATE account_averages SET credit = $2 WHERE accountID=$1",
				changedMsg.AggregateID(),
				accountState.CreditAmount/accountState.CreditCount,
			)
			return accountState, err
		},
		lib.BankAccountDebitedEventName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			changedMsg := message.(*aggregate.Changed)
			event := message.Payload().(lib.BankAccountCredited)

			accountState := state.(AccountAverageState)
			accountState.DebitAmount += event.Amount
			accountState.DebitCount++

			_, err := p.db.ExecContext(ctx,
				"UPDATE account_averages SET debit = $2 WHERE accountID=$1",
				changedMsg.AggregateID(),
				accountState.CreditAmount/accountState.CreditCount,
			)
			return accountState, err
		},
	}
}
