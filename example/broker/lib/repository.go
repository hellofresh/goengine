package lib

import (
	"context"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
)

// BankAccountTypeName is the name used to identify a bank account within the event store
const BankAccountTypeName = "bank_account"

// BankAccountRepository is a repository for bank accounts
type BankAccountRepository struct {
	repo *aggregate.Repository
}

// NewBankAccountRepository create a new BankAccountRepository
func NewBankAccountRepository(store goengine.EventStore, name goengine.StreamName) (*BankAccountRepository, error) {
	// Create a a aggregate.Type to allow the repository to reconstitute the BankAccount
	bankAccountType, err := aggregate.NewType(BankAccountTypeName, func() aggregate.Root { return &BankAccount{} })
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
