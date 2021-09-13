package main

import (
	"errors"

	"github.com/hellofresh/goengine/v2/aggregate"
)

var (
	// ErrInsufficientMoney occurs when a bank account has insufficient funds
	ErrInsufficientMoney = errors.New("insufficient money")
	// Ensure BankAccount implements the aggregate.Root interface
	_ aggregate.Root = &BankAccount{}
)

type (
	// BankAccount a simple AggregateRoot representing a BankAccount
	BankAccount struct {
		aggregate.BaseRoot

		accountID aggregate.ID
		balance   uint
	}

	// AccountOpened a DomainEvent indicating that a bank account was opened
	AccountOpened struct {
		AccountID aggregate.ID `json:"account_id"`
	}

	// AccountCredited a DomainEvent indicating that a bank account was credited
	AccountCredited struct {
		Amount uint `json:"amount"`
	}

	// AccountDebited a DomainEvent indicating that a bank account was debited
	AccountDebited struct {
		Amount uint `json:"amount"`
	}
)

// OpenBankAccount opens a new bank account
func OpenBankAccount() (*BankAccount, error) {
	accountID := aggregate.GenerateID()

	account := &BankAccount{
		accountID: accountID,
	}

	if err := aggregate.RecordChange(account, AccountOpened{AccountID: accountID}); err != nil {
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
	case AccountOpened:
		b.accountID = event.AccountID
	case AccountCredited:
		b.balance += event.Amount
	case AccountDebited:
		b.balance -= event.Amount
	}
}

// Deposit adds an amount of money to the bank account
func (b *BankAccount) Deposit(amount uint) error {
	if amount == 0 {
		return nil
	}

	return aggregate.RecordChange(b, AccountCredited{Amount: amount})
}

// Withdraw removes an amount of money to the bank account
func (b *BankAccount) Withdraw(amount uint) error {
	if amount > b.balance {
		return ErrInsufficientMoney
	}

	return aggregate.RecordChange(b, AccountDebited{Amount: amount})
}

// Balance returns the current amount of money that is contained in bank account
func (b *BankAccount) Balance() uint {
	return b.balance
}
