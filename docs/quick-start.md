# Getting Started

In this getting started guide we will go through creating a very basic banking application.

So let's get started building our awesome new Bank.

**Please remember that the code below is for example purposes and (may) not be production ready!**

## Creating the BankAccount

Our new bank will have customer bank accounts so let's define our BankAccount aggregate root.

```golang
import "github.com/hellofresh/goengine/aggregate"

// Ensure BankAccount implements the aggregate.Root interface
var _ aggregate.Root = &BankAccount{}

// BankAccount a simple AggregateRoot representing a BankAccount
type BankAccount struct {
    aggregate.BaseRoot

    accountID aggregate.ID
}

// AggregateID returns the bank accounts aggregate.ID
func (b *BankAccount) AggregateID() aggregate.ID {
    return b.accountID
}

// Apply changes the state of the BankAccount
func (b *BankAccount) Apply(change *aggregate.Changed) {
}
```

Now that we defined our BankAccount we need to allow our customers to open a bank account within our bank.
Before we can implement the `OpenBankAccount` action we first need to define the `BankAccountOpened` event.  

```golang
// BankAccountOpened a DomainEvent indicating that a bank account was opened
type BankAccountOpened struct {
    AccountID aggregate.ID `json:"account_id"`
}
```

Great so now we have the `BankAccountOpened` event and since we are event sourcing we need to apply the state change when the event is triggered.
In order to apply the state change (needed to reconstruct the BankAcount at a later stage) we need to modify the `Apply` func on the BankAccount.

```golang
// Apply changes the state of the BankAccount
func (b *BankAccount) Apply(change *aggregate.Changed) {
    switch event := change.Payload().(type) {
    case BankAccountOpened:
        b.accountID = event.AccountID
    }
}
```

Now we are finally ready to implement the `OpenBankAccount` action. Let's go!

```golang
// OpenBankAccount opens a new bank account
func OpenBankAccount() (*BankAccount, error) {
    accountID := aggregate.GenerateID()

    account := &BankAccount{accountID: accountID}

    if err := aggregate.RecordChange(account, BankAccountOpened{AccountID: accountID}); err != nil {
        return nil, err
    }
    return account, nil
}
```

Great so now we can finally open a bank account.
**Hold on a second** why are we setting the `accountID` on the `BankAccount` and then record the event?
Don't we already set the `accountID` in the `Apply` func? This may indeed seems like a bug in GoEngine but in order to
avoid a nice bit of reflection or interfaces on events GoEngine `aggregate.RecordChange` requires the provided aggregate root to have a `AggregateID`.

Now that our customers have a BankAccount we want them to be able to Deposit and Withdraw money from there account.
But we prefer to always keep them in the green so we will not allow them to debit there account for more then there current balance.

```golang
import "errors"

// BankAccountCredited a DomainEvent indicating that a bank account was credited
type BankAccountCredited struct {
    Amount uint `json:"amount"`
}

// BankAccountDebited a DomainEvent indicating that a bank account was debited
type BankAccountDebited struct {
    Amount uint `json:"amount"`
}

type BankAccount struct {
    // ...
    balance   uint
}

// Apply changes the state of the BankAccount based on the aggregate.Changed message
func (b *BankAccount) Apply(change *aggregate.Changed) {
    switch event := change.Payload().(type) {
    // ...
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
        return errors.New("insufficient money")
    }

    return aggregate.RecordChange(b, BankAccountDebited{Amount: amount})
}
```

Great we are done with our bank account let's take a look at how we can store the aggregate root

## Saving and Loading the bank account

GoEngine provides a [aggregate repository] to save and load aggregate roots.

In order to make loading and saving as easy within the application we advise you to create a little wrapper that will do the type casting for each aggregate root. So let's define the BankAccount Repository.

```golang
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
func NewBankAccountRepository(store goengine.EventStore, name goengine.StreamName) (
    *BankAccountRepository, error) {
    // Create a a aggregate.Type to allow the repository to reconstitute the BankAccount
    bankAccountType, err := aggregate.NewType(BankAccountTypeName, func() aggregate.Root {
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
func (r *BankAccountRepository) Get(ctx context.Context, aggregateID aggregate.ID) (
    *BankAccount, error) {
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
```

Now that we have a BankAccountRepository we need to configure the Event Store which manages the events for that the aggregate root.
*Currently GoEngine support a postgres and inmemory event store.*

To make your live easier we will use the postgres json SingleStreamManager. This manager is a helper so you don't need
to create a payload transformer, persistence strategy, message factory and finally the event store.

```golang
import (
    "context"
    "database/sql"
    "os"

    "github.com/hellofresh/goengine"
    "github.com/hellofresh/goengine/strategy/json"
    "github.com/hellofresh/goengine/strategy/json/sql/postgres"
)

func main() {
    postgresDSN := os.Getenv("POSTGRES_DSN")
    postgresDB, err := sql.Open("postgres", postgresDSN)

    manager, err := postgres.NewSingleStreamManager(postgresDB, goengine.NopLogger)
    if err != nil {
        panic(err)
    }

    // Register your events so that can be properly loaded from the event store
    if err := manager.RegisterPayloads(map[string]json.PayloadInitiator{
        "bank_account_opened": func() interface{} {
            return BankAccountOpened{}
        },
        "bank_account_credited": func() interface{} {
            return BankAccountCredited{}
        },
        "bank_account_debited": func() interface{} {
            return BankAccountDebited{}
        },
    }); err != nil {
        panic(err)
    }

    eventStore, err := manager.NewEventStore()
    if err != nil {
        panic(err)
    }

    // Create the needed Tables, indexes etc for the eventstore in the database
    // You probably don't want to do this for your production application. You can generate the needed SQL for your
    // db migrations as follows:
    // ```golang
    // tableName, err := manager.PersistenceStrategy().GenerateTableName("back_account_event_stream")
    // queries := manager.PersistenceStrategy().CreateSchema(tableName)
    // ```
    if err := eventStore.Create(context.Background(), "back_account_event_stream"); err != nil {
        panic(err)
    }

    bankAccountRepo, err := NewBankAccountRepository(eventStore, "back_account_event_stream")
    if err != nil {
        panic(err)
    }
}
```

Great we now have a event store, aggregate repository and aggregate root so we can finally start using them.

```golang
func main() {
    // ...
    ctx := context.Background()

    myFirstBankAccount, err := OpenBankAccount()
    if err != nil {
        panic(err)
    }
    if err := myFirstBankAccount.Deposit(100); err != nil {
        panic(err)
    }

    if err := bankAccountRepo.Save(ctx, myFirstBankAccount); err != nil {
        panic(err)
    }
}
```

Good Luck and if you have any suggestions or idea's to make this documentation better please submit a [Issue or Pull Request][repo]!  

## Creating reports

Now that we have our bank up and running it would be nice to know how much money the Bank in total holds.
To achieve this goal we need to create a total balance for the entire bank and update it based on every
`BankAccountCredited` and `BankAccountDebited` amount.

GoEngine provides a StreamProjector that allows your Projection to receive events that where appended to a event stream.

Let's start by building out the projection.

```golang
import (
    "context"
    "database/sql"

    "github.com/hellofresh/goengine"
)

var _ goengine.Projection = &BankTotalsProjection{}

type BankTotalsProjection struct {
    db *sql.DB
}

func NewBankTotalsProjection(db *sql.DB) *BankTotalsProjection {
    return &BankTotalsProjection{db: db}
}

func (*BankTotalsProjection) Name() string {
    return "bank_totals"
}

func (*BankTotalsProjection) FromStream() goengine.StreamName {
    return "back_account_event_stream"
}

func (*BankTotalsProjection) Init(ctx context.Context) (interface{}, error) {
    return nil, nil
}

func (p *BankTotalsProjection) Handlers() map[string]goengine.MessageHandler {
    return map[string]goengine.MessageHandler{
        "bank_account_credited": func(ctx context.Context, _ interface{}, message goengine.Message) (interface{}, error) {
            event := message.Payload().(BankAccountCredited)
            return p.db.ExecContext(ctx, "UPDATE bank_totals SET balance = balance + $1", event.Amount)
        },
        "bank_account_debited": func(ctx context.Context, _ interface{}, message goengine.Message) (interface{}, error) {
            event := message.Payload().(BankAccountCredited)
            return p.db.ExecContext(ctx, "UPDATE bank_totals SET balance = balance - $1", event.Amount)
        },
    }
}
```

Great now that we have our projection let's run it and listen to the event store.
```golang
import (
    "time"

    driverSQL "github.com/hellofresh/goengine/driver/sql"
    "github.com/hellofresh/goengine/extension/pq"
)

func main() {
    // ...
    
    var manager postgres.SingleStreamStrategy
        
    projector, err := manager.NewStreamProjector(
        "bank_account_projections", 
        NewBankTotalsProjection(postgresDB),
        func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
            return driverSQL.ProjectionFail
        },
    )     
    if err != nil {
        panic(err)
    }

    listener, err := pq.NewListener(postgresDSN, "back_account_event_stream", time.Millisecond, time.Second, goengine.NopLogger)
    if err != nil {
        panic(err)
    }
    
    if err := projector.RunAndListen(ctx, listener); err != nil {
        panic(err)
    }
}
```
*In production environments it's a good idea to run any projection separate from the main application, such as having a separated application binary only responsible for running the projections.*

[repo]: https://github.com/hellofresh/goengine
[aggregate repository]: https://github.com/hellofresh/goengine/tree/master/aggregate/repository.go
