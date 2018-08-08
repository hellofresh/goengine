package main

import (
	"context"
	"fmt"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/sirupsen/logrus"
)

func main() {
	logger := logrus.New()
	ctx := context.Background()

	store := inmemory.NewEventStore(logger)

	eventStream := eventstore.StreamName("event_stream")
	store.Create(eventStream)

	repository, err := NewBankAccountRepository(store, eventStream)
	failOnErr(err)

	fmt.Print("Opening an account\n")
	account := OpenBankAccount()
	accountID := account.AggregateID()
	failOnErr(
		repository.Save(ctx, account),
	)
	fmt.Printf("Opened account %s balance %d\n\n", account.AggregateID(), account.Balance())

	fmt.Print("Opening an account with 100\n")
	secondAccount := OpenBankAccount()
	secondAccount.Deposit(100)
	failOnErr(
		repository.Save(ctx, secondAccount),
	)
	fmt.Printf("Opened account %s balance %d\n\n", secondAccount.AggregateID(), secondAccount.Balance())

	fmt.Printf("Deposit 100 into account %s\n", accountID)
	loadedAccount, err := repository.Get(ctx, accountID)
	failOnErr(err)

	loadedAccount.Deposit(100)
	failOnErr(
		repository.Save(ctx, loadedAccount),
	)
	fmt.Printf("Opened account instance %s balance %d\n", account.AggregateID(), account.Balance())
	fmt.Printf("Loaded account instance %s balance %d\n\n", account.AggregateID(), loadedAccount.Balance())

	fmt.Printf("Withdraw 50 from account %s\n", accountID)
	loadedAccount, err = repository.Get(ctx, accountID)
	failOnErr(err)

	loadedAccount.Withdraw(50)
	failOnErr(
		repository.Save(ctx, loadedAccount),
	)
	fmt.Printf("Opened account instance %s balance %d\n", account.AggregateID(), account.Balance())
	fmt.Printf("Loaded account instance %s balance %d\n", account.AggregateID(), loadedAccount.Balance())
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
