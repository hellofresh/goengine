package main

import (
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/serializer"
	"github.com/satori/go.uuid"

	"gopkg.in/mgo.v2"
)

func main() {
	var streamName eventstore.StreamName = "test"
	aggregateID := uuid.NewV4()

	mongoDSN := os.Getenv("STORAGE_DSN")
	log.Infof("Connecting to the database %s", mongoDSN)
	session, err := mgo.Dial(mongoDSN)
	if err != nil {
		log.Panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	log.Info("Setting up the event store")
	registry := serializer.NewInMemmoryTypeRegistry()
	registry.Register(&SomethingHappened{})

	es := SetupEventStore(session, registry)

	log.Info("Creating the event stream")
	stream := CreateEventStream(streamName, aggregateID.String())

	err = es.Append(stream)
	if nil != err {
		log.Error(err)
	}

	events, err := es.GetEventsFor(streamName, aggregateID.String())
	if nil != err {
		log.Error(err)
	}
	log.Info(events)
}

func SetupEventStore(session *mgo.Session, registry serializer.TypeRegistry) eventstore.EventStore {
	adapter := eventstore.NewMongoDbEventStore(session, registry)
	return eventstore.NewEventStore(adapter)
}

func CreateEventStream(streamName eventstore.StreamName, aggregateId string) *eventstore.EventStream {
	var events []*eventstore.DomainMessage

	events = append(events, eventstore.RecordNow(aggregateId, 0, NewSomethingHappened()))
	events = append(events, eventstore.RecordNow(aggregateId, 1, NewSomethingHappened()))
	events = append(events, eventstore.RecordNow(aggregateId, 2, NewSomethingHappened()))

	return eventstore.NewEventStream(streamName, events)
}
