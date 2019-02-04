package main

import (
	"context"

	goengine_dev "github.com/hellofresh/goengine-dev"

	"github.com/hellofresh/goengine/aggregate"
)

// BankAccountRepository is a repository for bank accounts
type BankAccountRepository struct {
	repo *aggregate.Repository
}

// NewBankAccountRepository create a new BankAccountRepository
func NewBankAccountRepository(store goengine_dev.EventStore, name goengine_dev.StreamName) (*BankAccountRepository, error) {
	bankAccountType, err := aggregate.NewType("bank_account", func() aggregate.Root {
		return &BankAccount{}
	})
	if err != nil {
		return nil, err
	}

	repo, err := aggregate.NewRepository(store, name, bankAccountType)
	if err != nil {
		return nil, err
	}

	return &BankAccountRepository{repo}, nil
}

// Get loads the bank account
func (r *BankAccountRepository) Get(ctx context.Context, aggregateID aggregate.ID) (*BankAccount, error) {
	root, err := r.repo.GetAggregateRoot(ctx, aggregateID)
	if err != nil {
		return nil, err
	}

	bankAccount := root.(*BankAccount)

	return bankAccount, nil
}

// Save the bank account
func (r *BankAccountRepository) Save(ctx context.Context, bankAccount *BankAccount) error {
	return r.repo.SaveAggregateRoot(ctx, bankAccount)
}
