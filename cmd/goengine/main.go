package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/eventsourcing"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/mongodb"

	"gopkg.in/mgo.v2"
)

func main() {
	log.SetLevel(log.DebugLevel)
	var streamName eventstore.StreamName = "test"

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

	log.Info("Setting up the event store")
	es := mongodb.NewEventStore(session, registry)

	log.Info("Creating a recipe")
	repository := eventsourcing.NewPublisherRepository(es)

	log.Info("Creating a recipe")
	aggregateRoot := CreateScenario(streamName)

	repository.Save(aggregateRoot, streamName)

	history, err := NewRecipeFromHisotry(aggregateRoot.ID, streamName, repository)
	if err != nil {
		log.Panic(err)
	}

	log.Info(history)
}

func CreateScenario(streamName eventstore.StreamName) *Recipe {
	recipe := NewRecipe("Test Recipe")
	recipe.Rate(4)
	return recipe
}
