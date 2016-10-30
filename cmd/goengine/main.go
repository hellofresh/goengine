package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/inmemory"
	"github.com/hellofresh/goengine/mongodb"

	"gopkg.in/mgo.v2"
)

func main() {
	log.SetLevel(log.DebugLevel)
	var streamName goengine.StreamName = "test"

	mongoDSN := os.Getenv("STORAGE_DSN")
	log.Infof("Connecting to the database %s", mongoDSN)
	session, err := mgo.Dial(mongoDSN)
	if err != nil {
		log.Panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	log.Info("Setting up the registry")
	registry := goengine.NewInMemmoryTypeRegistry()
	registry.RegisterType(&RecipeCreated{})
	registry.RegisterType(&RecipeRated{})

	log.Info("Setting up the event bus")
	bus := inmemory.NewInMemoryEventBus()

	log.Info("Setting up the event store")
	es := mongodb.NewEventStore(session, registry)

	log.Info("Creating a recipe")
	repository := goengine.NewPublisherRepository(es, bus)

	eventDispatcher := goengine.NewVersionedEventDispatchManager(bus, registry)

	eventDispatcher.RegisterEventHandler(&RecipeCreated{}, func(event *goengine.DomainMessage) error {
		log.Debug("Event received")
		return nil
	})

	stopChannel := make(chan bool)
	go eventDispatcher.Listen(stopChannel, false)

	log.Info("Creating a recipe")
	aggregateRoot := CreateScenario(streamName)

	repository.Save(aggregateRoot, streamName)

	history, err := NewRecipeFromHisotry(aggregateRoot.ID, streamName, repository)
	if err != nil {
		log.Panic(err)
	}

	log.Println("Stop channel")
	stopChannel <- true

	log.Info(history)
}

func CreateScenario(streamName goengine.StreamName) *Recipe {
	recipe := NewRecipe("Test Recipe")
	recipe.Rate(4)
	return recipe
}
