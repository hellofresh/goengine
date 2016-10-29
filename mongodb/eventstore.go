package mongodb

import (
	"encoding/json"
	"time"

	"github.com/hellofresh/goengine/errors"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/reflection"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type EventData struct {
	ID         string    `bson:"aggregate_id,omitempty"`
	Version    int       `bson:"version"`
	Type       string    `bson:"type"`
	Payload    string    `bson:"payload"`
	RecordedOn time.Time `bson:"recorded_on"`
}

type MongoDbEventStore struct {
	conn     *mgo.Session
	db       *mgo.Database
	registry reflection.TypeRegistry
}

func NewEventStore(conn *mgo.Session, r reflection.TypeRegistry) *MongoDbEventStore {
	db := conn.DB("")
	return &MongoDbEventStore{conn, db, r}
}

func (s *MongoDbEventStore) Append(events *eventstore.EventStream) error {
	name := events.Name
	for _, event := range events.Events {
		err := s.save(name, event)
		if nil != err {
			return err
		}
	}

	return nil
}

func (s *MongoDbEventStore) GetEventsFor(streamName eventstore.StreamName, id string) (*eventstore.EventStream, error) {
	var eventsData []*EventData
	var results []*eventstore.DomainMessage

	coll := s.db.C(string(streamName))

	err := coll.Find(bson.M{"aggregate_id": id}).All(&eventsData)

	for _, eventData := range eventsData {
		event, err := s.registry.Get(eventData.Type)
		if nil != err {
			return nil, err
		}

		err = json.Unmarshal([]byte(eventData.Payload), event)
		if nil != err {
			return nil, err
		}

		domainMessage := eventstore.NewDomainMessage(eventData.ID, eventData.Version, event.(eventstore.DomainEvent), eventData.RecordedOn)
		results = append(results, domainMessage)
	}

	return eventstore.NewEventStream(streamName, results), err
}

func (s *MongoDbEventStore) FromVersion(streamName eventstore.StreamName, id string, version int) (*eventstore.EventStream, error) {
	var results []*eventstore.DomainMessage
	coll := s.db.C(string(streamName))

	err := coll.Find(bson.M{
		"aggregate_id": id,
		"version":      bson.M{"$gte": version},
	}).
		Sort("-version").
		All(&results)

	return eventstore.NewEventStream(streamName, results), err
}

func (s *MongoDbEventStore) CountEventsFor(streamName eventstore.StreamName, id string) (int, error) {
	return s.db.C(string(streamName)).Find(bson.M{"aggregate_id": string(streamName)}).Count()
}

func (s *MongoDbEventStore) createIndexes(c *mgo.Collection) error {
	index := mgo.Index{
		Key:        []string{"aggregate_id", "version"},
		Unique:     true,
		DropDups:   true,
		Background: true,
	}

	return c.EnsureIndex(index)
}

func (s *MongoDbEventStore) save(streamName eventstore.StreamName, event *eventstore.DomainMessage) error {
	coll := s.db.C(string(streamName))
	err := s.createIndexes(coll)
	if nil != err {
		return err
	}

	serializedPayload, err := json.Marshal(event.Payload)
	if nil != err {
		return err
	}

	typeName, err := s.registry.TypeOf(event.Payload)
	if nil != err {
		return err
	}

	if !s.registry.Exists(typeName.String()) {
		return errors.ErrorTypeNotRegistred
	}

	eventData := &EventData{
		ID:         event.ID,
		Version:    event.Version,
		Type:       typeName.String(),
		Payload:    string(serializedPayload),
		RecordedOn: event.RecordedOn,
	}

	return coll.Insert(eventData)
}
