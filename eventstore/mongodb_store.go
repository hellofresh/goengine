package eventstore

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/hellofresh/goengine/errors"
	"github.com/hellofresh/goengine/serializer"

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
	registry serializer.TypeRegistry
}

func NewMongoDbEventStore(conn *mgo.Session, r serializer.TypeRegistry) *MongoDbEventStore {
	db := conn.DB("")
	return &MongoDbEventStore{conn, db, r}
}

func (s *MongoDbEventStore) Save(streamName StreamName, event *DomainMessage) error {
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

func (s *MongoDbEventStore) GetEventsFor(streamName StreamName, id string) ([]*DomainMessage, error) {
	var eventsData []*EventData
	var results []*DomainMessage

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

		domainMessage := NewDomainMessage(eventData.ID, eventData.Version, event.(DomainEvent), eventData.RecordedOn)
		log.Info(domainMessage.Payload)
		results = append(results, domainMessage)
	}

	return results, err
}

func (s *MongoDbEventStore) FromVersion(streamName StreamName, id string, version int) ([]*DomainMessage, error) {
	var results []*DomainMessage
	coll := s.db.C(string(streamName))

	err := coll.Find(bson.M{
		"aggregate_id": id,
		"version":      bson.M{"$gte": version},
	}).
		Sort("-version").
		All(&results)

	return results, err
}

func (s *MongoDbEventStore) CountEventsFor(streamName StreamName, id string) (int, error) {
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
