package projection

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/example/bank/config"
	"github.com/hellofresh/goengine/example/bank/domain"
)

var _ goengine.ProjectionSaga = &AccountAverageProjection{}

type (
	AccountAverageProjection struct {
		db *sql.DB
	}

	AccountAverageState struct {
		CreditCount  uint
		CreditAmount uint
		DebitCount   uint
		DebitAmount  uint
	}
)

func (p *AccountAverageProjection) DecodeState(data []byte) (interface{}, error) {
	var state AccountAverageState
	err := json.Unmarshal(data, &state)
	return state, err
}

func (p *AccountAverageProjection) EncodeState(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

func NewAccountAverageProjection(db *sql.DB) *AccountAverageProjection {
	return &AccountAverageProjection{db: db}
}

func (*AccountAverageProjection) Name() string {
	return "account_averages"
}

func (*AccountAverageProjection) FromStream() goengine.StreamName {
	return config.EventStoreStreamName
}

func (p *AccountAverageProjection) Init(ctx context.Context) (interface{}, error) {
	return AccountAverageState{}, nil
}

func (p *AccountAverageProjection) Handlers() map[string]goengine.MessageHandler {
	return map[string]goengine.MessageHandler{
		domain.BankAccountOpenedName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			_, err := p.db.ExecContext(ctx,
				"INSERT INTO account_averages (accountID, credit, debit) VALUES ($1, 0, 0)",
				message.Payload().(domain.BankAccountOpened).AccountID,
			)
			return state, err
		},
		domain.BankAccountCreditedName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			changedMsg := message.(*aggregate.Changed)
			event := message.Payload().(domain.BankAccountCredited)

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
		domain.BankAccountDebitedName: func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			changedMsg := message.(*aggregate.Changed)
			event := message.Payload().(domain.BankAccountCredited)

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
