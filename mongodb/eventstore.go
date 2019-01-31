package mongodb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/reflection"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

// MongoEvent represents an event on mongodb
type MongoEvent struct {
	ID         string    `bson:"aggregate_id,omitempty"`
	Version    int       `bson:"version"`
	Type       string    `bson:"type"`
	Payload    string    `bson:"payload"`
	RecordedOn time.Time `bson:"recorded_on"`
}

// EventStore The mongodb event store
type EventStore struct {
	mongoDB *mongo.Database

	registry goengine.TypeRegistry
}

// NewEventStore creates new MongoDB based event store
func NewEventStore(mongoDB *mongo.Database, r goengine.TypeRegistry) *EventStore {
	return &EventStore{mongoDB, r}
}

// Append adds an event to the event store
func (s *EventStore) Append(events *goengine.EventStream) error {
	streamName := string(events.Name)
	for _, event := range events.Events {
		mongoEvent, err := s.toMongoEvent(event)
		if nil != err {
			return err
		}

		coll := s.mongoDB.Collection(streamName)
		err = s.createIndexes(coll)
		if nil != err {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = coll.InsertOne(ctx, mongoEvent)
		cancel()

		if nil != err {
			return err
		}
	}

	return nil
}

// GetEventsFor gets events for an id on the specified stream
func (s *EventStore) GetEventsFor(streamName goengine.StreamName, id string) (*goengine.EventStream, error) {
	var mongoEvents []MongoEvent
	coll := s.mongoDB.Collection(string(streamName))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cur, err := coll.Find(ctx, bson.M{"aggregate_id": id})
	if err != nil {
		return nil, err
	}

	for cur.Next(ctx) {
		var mongoEvent MongoEvent
		err := cur.Decode(&mongoEvent)
		if err != nil {
			return nil, err
		}

		mongoEvents = append(mongoEvents, mongoEvent)
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}

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
func (s *EventStore) FromVersion(streamName goengine.StreamName, id string, version int) (*goengine.EventStream, error) {
	var mongoEvents []MongoEvent
	coll := s.mongoDB.Collection(string(streamName))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cur, err := coll.Find(
		ctx,
		bson.M{
			"aggregate_id": id,
			"version":      bson.M{"$gte": version},
		},
		options.Find().SetSort("-version"),
	)
	if err != nil {
		return nil, err
	}

	for cur.Next(ctx) {
		var mongoEvent MongoEvent
		err := cur.Decode(mongoEvent)
		if err != nil {
			return nil, err
		}

		mongoEvents = append(mongoEvents, mongoEvent)
	}

	if err := cur.Err(); err != nil {
		return nil, err
	}

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
func (s *EventStore) CountEventsFor(streamName goengine.StreamName, id string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.mongoDB.Collection(string(streamName)).Count(ctx, bson.M{"aggregate_id": string(streamName)})
}

func (s *EventStore) createIndexes(c *mongo.Collection) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys: bsonx.Doc{
				{Key: "aggregate_id", Value: bsonx.Int32(1)},
				{Key: "version", Value: bsonx.Int32(-1)},
			},
			Options: options.Index().SetUnique(true).SetBackground(true),
		},
	)
	return err
}

func (s *EventStore) toMongoEvent(event *goengine.DomainMessage) (*MongoEvent, error) {
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

func (s *EventStore) fromMongoEvent(mongoEvent MongoEvent) (*goengine.DomainMessage, error) {
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
