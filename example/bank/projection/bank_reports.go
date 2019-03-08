package projection

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/example/bank/config"
	"github.com/hellofresh/goengine/example/bank/domain"
	"github.com/lib/pq"
)

var _ goengine.Projection = &BankReportProjection{}

// BankReportProjection the general bank report projection
type BankReportProjection struct {
	db *sql.DB
}

// NewBankReportProjection returns a new BankReportProjection
func NewBankReportProjection(db *sql.DB) *BankReportProjection {
	return &BankReportProjection{db: db}
}

// Name return the name of the projection
func (*BankReportProjection) Name() string {
	return "bank_reports"
}

// FromStream returns the name of the stream that this projection uses
func (*BankReportProjection) FromStream() goengine.StreamName {
	return config.EventStoreStreamName
}

// Init initializes the projection by creating db table for this projection and it's related rows
// These rows will later be populated by the handlers
func (p *BankReportProjection) Init(ctx context.Context) (interface{}, error) {
	_, err := p.db.Exec(`CREATE TABLE bank_reports (name VARCHAR(20) UNIQUE NOT NULL, amount bigint default 0 not null);`)
	if err != nil {
		// Ignore duplicate table warnings
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42P07" {
			return nil, nil
		}

		return nil, err
	}

	_, err = p.db.Exec("INSERT INTO bank_reports (name, amount) VALUES ('balance', 0), ('accounts', 0)")
	return nil, err
}

// Handlers return the handlers for the events we want to project
func (p *BankReportProjection) Handlers() map[string]goengine.MessageHandler {
	return map[string]goengine.MessageHandler{
		domain.BankAccountOpenedName: func(ctx context.Context, _ interface{}, _ goengine.Message) (interface{}, error) {
			_, err := p.db.ExecContext(ctx, "UPDATE bank_reports SET amount = amount + 1 WHERE name='accounts'")
			return nil, err
		},
		domain.BankAccountCreditedName: func(ctx context.Context, _ interface{}, message goengine.Message) (interface{}, error) {
			event := message.Payload().(domain.BankAccountCredited)
			_, err := p.db.ExecContext(ctx, "UPDATE bank_reports SET amount = amount + $1 WHERE name='balance'", event.Amount)
			return nil, err
		},
		domain.BankAccountDebitedName: func(ctx context.Context, _ interface{}, message goengine.Message) (interface{}, error) {
			event := message.Payload().(domain.BankAccountCredited)
			_, err := p.db.ExecContext(ctx, "UPDATE bank_reports SET amount = amount - $1 WHERE name='balance'", event.Amount)
			return nil, err
		},
	}
}
