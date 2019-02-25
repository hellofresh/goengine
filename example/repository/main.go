package main

import (
	"context"
	"fmt"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/inmemory"
	"github.com/hellofresh/goengine/extension/logrus"
)

func main() {
	logger := logrus.StandardLogger()
	ctx := context.Background()

	store := inmemory.NewEventStore(logger)

	eventStream := goengine.StreamName("event_stream")
	failOnErr(
		store.Create(ctx, eventStream),
	)

	repository, err := NewBankAccountRepository(store, eventStream)
	failOnErr(err)

	fmt.Print("Opening an account\n")
	account, err := OpenBankAccount()
	failOnErr(err)
	failOnErr(
		repository.Save(ctx, account),
	)
	accountID := account.AggregateID()
	fmt.Printf("Opened account %s balance %d\n\n", account.AggregateID(), account.Balance())

	fmt.Print("Opening an account with 100\n")
	secondAccount, err := OpenBankAccount()
	failOnErr(err)
	failOnErr(
		secondAccount.Deposit(100),
	)
	failOnErr(
		repository.Save(ctx, secondAccount),
	)
	fmt.Printf("Opened account %s balance %d\n\n", secondAccount.AggregateID(), secondAccount.Balance())

	fmt.Printf("Deposit 100 into account %s\n", accountID)
	loadedAccount, err := repository.Get(ctx, accountID)
	failOnErr(err)

	failOnErr(
		loadedAccount.Deposit(100),
	)
	failOnErr(
		repository.Save(ctx, loadedAccount),
	)
	fmt.Printf("Opened account instance %s balance %d\n", account.AggregateID(), account.Balance())
	fmt.Printf("Loaded account instance %s balance %d\n\n", account.AggregateID(), loadedAccount.Balance())

	fmt.Printf("Withdraw 50 from account %s\n", accountID)
	loadedAccount, err = repository.Get(ctx, accountID)
	failOnErr(err)

	failOnErr(
		loadedAccount.Withdraw(50),
	)
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
