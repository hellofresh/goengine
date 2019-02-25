package domain

import (
	"errors"

	"github.com/hellofresh/goengine/aggregate"
)

var (
	// ErrInsufficientFunds occurs when a bank account withdrawal exceeds it's balance
	ErrInsufficientFunds = errors.New("insufficient funds")

	// Ensure BankAccount implements the aggregate.Root interface
	_ aggregate.Root = &BankAccount{}
)

const (
	// BankAccountOpenedName is the event name for BankAccountOpened
	BankAccountOpenedName = "bank_account_opened"
	// BankAccountCreditedName is the event name for BankAccountCredited
	BankAccountCreditedName = "bank_account_credited"
	// BankAccountDebitedName is the event name for BankAccountDebited
	BankAccountDebitedName = "bank_account_debited"
)

type (
	// BankAccount a simple AggregateRoot representing a BankAccount
	BankAccount struct {
		aggregate.BaseRoot

		accountID aggregate.ID
		balance   uint
	}

	// BankAccountOpened a DomainEvent indicating that a bank account was opened
	BankAccountOpened struct {
		AccountID aggregate.ID `json:"account_id"`
	}

	// BankAccountCredited a DomainEvent indicating that a bank account was credited
	BankAccountCredited struct {
		Amount uint `json:"amount"`
	}

	// BankAccountDebited a DomainEvent indicating that a bank account was debited
	BankAccountDebited struct {
		Amount uint `json:"amount"`
	}
)

// OpenBankAccount opens a new bank account
func OpenBankAccount() (*BankAccount, error) {
	accountID := aggregate.GenerateID()

	account := &BankAccount{accountID: accountID}

	if err := aggregate.RecordChange(account, BankAccountOpened{AccountID: accountID}); err != nil {
		return nil, err
	}

	return account, nil
}

// AggregateID returns the bank accounts aggregate.ID
func (b *BankAccount) AggregateID() aggregate.ID {
	return b.accountID
}

// Deposit adds an amount of money to the bank account
func (b *BankAccount) Deposit(amount uint) error {
	if amount == 0 {
		return nil
	}

	return aggregate.RecordChange(b, BankAccountCredited{Amount: amount})
}

// Withdraw removes an amount of money to the bank account
func (b *BankAccount) Withdraw(amount uint) error {
	if amount > b.balance {
		return ErrInsufficientFunds
	}

	return aggregate.RecordChange(b, BankAccountDebited{Amount: amount})
}

// Apply changes the state of the BankAccount
func (b *BankAccount) Apply(change *aggregate.Changed) {
	switch event := change.Payload().(type) {
	case BankAccountOpened:
		b.accountID = event.AccountID
	case BankAccountCredited:
		b.balance += event.Amount
	case BankAccountDebited:
		b.balance -= event.Amount
	}
}
