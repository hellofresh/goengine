package lib

import (
	"errors"

	"github.com/hellofresh/goengine/aggregate"
)

var (
	// ErrInsufficientMoney occurs when a bank account has insufficient funds
	ErrInsufficientMoney = errors.New("insufficient money")
	// Ensure BankAccount implements the aggregate.Root interface
	_ aggregate.Root = &BankAccount{}
)

// BankAccount a simple AggregateRoot representing a BankAccount
type BankAccount struct {
	aggregate.BaseRoot

	accountID aggregate.ID
	balance   uint
}

// OpenBankAccount opens a new bank account
func OpenBankAccount() (*BankAccount, error) {
	accountID := aggregate.GenerateID()

	account := &BankAccount{
		accountID: accountID,
	}

	if err := aggregate.RecordChange(account, BankAccountOpened{AccountID: accountID}); err != nil {
		return nil, err
	}

	return account, nil
}

// AggregateID returns the bank accounts aggregate.ID need to implement aggregate.Root
func (b *BankAccount) AggregateID() aggregate.ID {
	return b.accountID
}

// Apply changes the state of the BankAccount based on the aggregate.Changed message
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
		return ErrInsufficientMoney
	}

	return aggregate.RecordChange(b, BankAccountDebited{Amount: amount})
}

// Balance returns the current amount of money that is contained in bank account
func (b *BankAccount) Balance() uint {
	return b.balance
}
