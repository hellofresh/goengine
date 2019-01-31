package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/mongodb"
	"github.com/hellofresh/goengine/rabbit"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

func main() {
	var streamName goengine.StreamName = "test"

	mongoDSN := os.Getenv("STORAGE_DSN")
	if len(mongoDSN) == 0 {
		panic(errors.New("missing STORAGE_DSN environment variable"))
	}
	brokerDSN := os.Getenv("BROKER_DSN")
	if len(mongoDSN) == 0 {
		panic(errors.New("missing BROKER_DSN environment variable"))
	}

	goengine.Log("Connecting to the database", map[string]interface{}{"dsn": mongoDSN}, nil)
	mongoClient, err := mongo.NewClientWithOptions(
		mongoDSN,
		options.Client().SetAppName("goengine"),
	)
	if err != nil {
		goengine.Log("Failed to create new Mongo mongoClient", nil, err)
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = mongoClient.Connect(ctx)
	if err != nil {
		goengine.Log("Failed to connect to Mongo", nil, err)
		panic(err)
	}

	defer func() {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Disconnect(ctx); err != nil {
			goengine.Log("Failed to close connection to Mongo", nil, err)
			panic(err)
		}
	}()

	goengine.Log("Setting up the registry", nil, nil)
	registry := goengine.NewInMemoryTypeRegistry()
	registry.RegisterType(&RecipeCreated{})
	registry.RegisterType(&RecipeRated{})

	goengine.Log("Setting up the event bus", map[string]interface{}{"dsn": brokerDSN}, nil)
	bus := rabbit.NewEventBus(brokerDSN, "events", "events")

	goengine.Log("Setting up the event store", nil, nil)
	eventStore := mongodb.NewEventStore(mongoClient.Database("event_store"), registry)

	eventDispatcher := goengine.NewVersionedEventDispatchManager(bus, registry)
	eventDispatcher.RegisterEventHandler(&RecipeCreated{}, func(event *goengine.DomainMessage) error {
		goengine.Log("Event received", map[string]interface{}{"event": event}, nil)
		return nil
	})

	stopChannel := make(chan bool)
	go eventDispatcher.Listen(stopChannel, false)

	goengine.Log("Creating a recipe", nil, nil)
	aggregateRoot := createScenario()

	repository := goengine.NewPublisherRepository(eventStore, bus)
	if err := repository.Save(aggregateRoot, streamName); err != nil {
		goengine.Log("Failed to save aggregate to stream", nil, err)
		panic(err)
	}

	_, err = NewRecipeFromHistory(aggregateRoot.ID, streamName, repository)
	if err != nil {
		goengine.Log("Failed get a recipe from history", nil, err)
		panic(err)
	}

	goengine.Log("Stopping channel", nil, err)
	stopChannel <- true
}

func createScenario() *Recipe {
	recipe := NewRecipe("Test Recipe")
	recipe.Rate(4)
	return recipe
}
