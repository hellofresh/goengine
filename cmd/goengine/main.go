package main

import (
	"os"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/mongodb"
	"github.com/hellofresh/goengine/rabbit"
	"gopkg.in/mgo.v2"
)

func main() {
	var streamName goengine.StreamName = "test"

	mongoDSN := os.Getenv("STORAGE_DSN")
	goengine.Log("Connecting to the database", map[string]interface{}{"dsn": mongoDSN}, nil)
	session, err := mgo.Dial(mongoDSN)
	if err != nil {
		goengine.Log("Failed to connect to Mongo", nil, err)
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	goengine.Log("Setting up the registry", nil, nil)
	registry := goengine.NewInMemoryTypeRegistry()
	registry.RegisterType(&RecipeCreated{})
	registry.RegisterType(&RecipeRated{})

	// bus := inmemory.NewInMemoryEventBus()
	brokerDSN := os.Getenv("BROKER_DSN")
	goengine.Log("Setting up the event bus", map[string]interface{}{"dsn": brokerDSN}, nil)
	bus := rabbit.NewEventBus(brokerDSN, "events", "events")

	goengine.Log("Setting up the event store", nil, nil)
	es := mongodb.NewEventStore(session, registry)

	eventDispatcher := goengine.NewVersionedEventDispatchManager(bus, registry)
	eventDispatcher.RegisterEventHandler(&RecipeCreated{}, func(event *goengine.DomainMessage) error {
		goengine.Log("Event received", nil, nil)
		return nil
	})

	stopChannel := make(chan bool)
	go eventDispatcher.Listen(stopChannel, false)

	goengine.Log("Creating a recipe", nil, nil)
	aggregateRoot := CreateScenario(streamName)

	repository := goengine.NewPublisherRepository(es, bus)
	repository.Save(aggregateRoot, streamName)

	_, err = NewRecipeFromHisotry(aggregateRoot.ID, streamName, repository)
	if err != nil {
		goengine.Log("Failed to connect to Mongo", nil, err)
		panic(err)
	}

	goengine.Log("Stopping channel", nil, err)
	stopChannel <- true
}

func CreateScenario(streamName goengine.StreamName) *Recipe {
	recipe := NewRecipe("Test Recipe")
	recipe.Rate(4)
	return recipe
}
