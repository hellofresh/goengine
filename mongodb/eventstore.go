package mongodb

import (
	"encoding/json"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/reflection"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongoEvent represents an event on mongodb
type MongoEvent struct {
	ID         string    `bson:"aggregate_id,omitempty"`
	Version    int       `bson:"version"`
	Type       string    `bson:"type"`
	Payload    string    `bson:"payload"`
	RecordedOn time.Time `bson:"recorded_on"`
}

// MongoDbEventStore The mongodb event store
type MongoDbEventStore struct {
	conn     *mgo.Session
	db       *mgo.Database
	registry goengine.TypeRegistry
}

// NewEventStore creates new MongoDB based event store
func NewEventStore(conn *mgo.Session, r goengine.TypeRegistry) *MongoDbEventStore {
	return &MongoDbEventStore{conn, conn.DB(""), r}
}

// Append adds an event to the event store
func (s *MongoDbEventStore) Append(events *goengine.EventStream) error {
	streamName := string(events.Name)
	for _, event := range events.Events {
		mongoEvent, err := s.toMongoEvent(event)
		if nil != err {
			return err
		}

		coll := s.db.C(streamName)
		err = s.createIndexes(coll)
		if nil != err {
			return err
		}

		err = coll.Insert(mongoEvent)
		if nil != err {
			return err
		}
	}

	return nil
}

// GetEventsFor gets events for an id on the specified stream
func (s *MongoDbEventStore) GetEventsFor(streamName goengine.StreamName, id string) (*goengine.EventStream, error) {
	var mongoEvents []*MongoEvent
	coll := s.db.C(string(streamName))
	err := coll.Find(bson.M{"aggregate_id": id}).All(&mongoEvents)

	var results []*goengine.DomainMessage
	for _, mongoEvent := range mongoEvents {
		domainMessage, err := s.fromMongoEvent(mongoEvent)
		if nil != err {
			return nil, err
		}

		results = append(results, domainMessage)
	}

	return goengine.NewEventStream(streamName, results), err
}

// FromVersion gets events for an id and version on the specified stream
func (s *MongoDbEventStore) FromVersion(streamName goengine.StreamName, id string, version int) (*goengine.EventStream, error) {
	var mongoEvents []*MongoEvent
	coll := s.db.C(string(streamName))

	err := coll.Find(bson.M{
		"aggregate_id": id,
		"version":      bson.M{"$gte": version},
	}).
		Sort("-version").
		All(&mongoEvents)

	var results []*goengine.DomainMessage
	for _, mongoEvent := range mongoEvents {
		domainMessage, err := s.fromMongoEvent(mongoEvent)
		if nil != err {
			return nil, err
		}

		results = append(results, domainMessage)
	}

	return goengine.NewEventStream(streamName, results), err
}

// CountEventsFor counts events for an id on the specified stream
func (s *MongoDbEventStore) CountEventsFor(streamName goengine.StreamName, id string) (int, error) {
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

func (s *MongoDbEventStore) toMongoEvent(event *goengine.DomainMessage) (*MongoEvent, error) {
	serializedPayload, err := json.Marshal(event.Payload)
	if nil != err {
		return nil, err
	}

	typeName := reflection.TypeOf(event.Payload)
	return &MongoEvent{
		ID:         event.ID,
		Version:    event.Version,
		Type:       typeName.String(),
		Payload:    string(serializedPayload),
		RecordedOn: event.RecordedOn,
	}, nil
}

func (s *MongoDbEventStore) fromMongoEvent(mongoEvent *MongoEvent) (*goengine.DomainMessage, error) {
	event, err := s.registry.Get(mongoEvent.Type)
	if nil != err {
		return nil, err
	}

	err = json.Unmarshal([]byte(mongoEvent.Payload), event)
	if nil != err {
		return nil, err
	}

	return goengine.NewDomainMessage(
		mongoEvent.ID,
		mongoEvent.Version,
		event.(goengine.DomainEvent),
		mongoEvent.RecordedOn,
	), nil
}
