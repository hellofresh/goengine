package main

import (
	"context"
	"fmt"
	"github.com/hellofresh/goengine/example/broker/lib"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
	"math/rand"
)

func main() {
	ctx := context.Background()

	logger, err := zap.NewDevelopment()
	failOnErr(err)

	db, dbCloser, err := lib.NewPostgresDB(logger)
	failOnErr(err)
	defer dbCloser()

	manager, err := lib.NewGoEngineManager(db, goengineZap.Wrap(logger))
	failOnErr(err)

	eventStore, err := manager.NewEventStore()
	failOnErr(err)

	repository, err := lib.NewBankAccountRepository(eventStore, lib.EventStoreStreamName)
	failOnErr(err)

	var accounts [10]*lib.BankAccount
	for i := 0; i < 10; i++ {
		account, err := lib.OpenBankAccount()
		failOnErr(err)
		failOnErr(repository.Save(ctx, account))

		failOnErr(account.Deposit(100));
		failOnErr(account.Withdraw(10));
		failOnErr(repository.Save(ctx, account))

		accounts[i] = account
	}

	for _, account := range accounts {
		failOnErr(account.Withdraw(uint(rand.Int31n(80))));
		failOnErr(repository.Save(ctx, account))
	}

	fmt.Println("BankAccount balances:")
	for _, account := range accounts {
		fmt.Printf("- %s: %d\n", account.AggregateID(), account.Balance())
	}
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
