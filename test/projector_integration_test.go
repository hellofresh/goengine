//go:build integration

package test_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/aggregate"
	"github.com/hellofresh/goengine/v2/driver/sql/postgres"
	"github.com/hellofresh/goengine/v2/metadata"
	strategyJSON "github.com/hellofresh/goengine/v2/strategy/json"
	strategySQL "github.com/hellofresh/goengine/v2/strategy/json/sql"
	strategyPostgres "github.com/hellofresh/goengine/v2/strategy/json/sql/postgres"
	"github.com/hellofresh/goengine/v2/test/internal"
)

const accountAggregateTypeName = "account"

var _ goengine.ProjectionSaga = &DepositedProjection{}

type (
	AccountCredited struct {
		Amount uint
	}

	AccountDeposited struct {
		Amount uint
	}

	DepositedProjection struct {
	}

	depositedProjectionState struct {
		Total       uint
		TotalAmount uint64
	}
)

func (p *DepositedProjection) Init(context.Context) (interface{}, error) {
	return depositedProjectionState{}, nil
}

func (p *DepositedProjection) Name() string {
	return "deposited_report"
}

func (p *DepositedProjection) FromStream() goengine.StreamName {
	return "event_stream"
}

func (p *DepositedProjection) Handlers() map[string]goengine.MessageHandler {
	return map[string]goengine.MessageHandler{
		"account_debited": func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			projectionState := state.(depositedProjectionState)

			switch event := message.Payload().(type) {
			case AccountDeposited:
				projectionState.Total++
				projectionState.TotalAmount += uint64(event.Amount)
			default:
				return state, errors.New("unexpected message payload type")
			}

			return projectionState, nil
		},
	}
}

func (p *DepositedProjection) DecodeState(data []byte) (interface{}, error) {
	var state depositedProjectionState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return state, nil
}

func (p *DepositedProjection) EncodeState(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

type projectorSuite struct {
	internal.PostgresSuite

	eventStream        goengine.StreamName
	eventStore         *postgres.EventStore
	eventStoreTable    string
	payloadTransformer *strategyJSON.PayloadTransformer
}

func (s *projectorSuite) SetupTest() {
	s.PostgresSuite.SetupTest()
	db := s.DB()

	s.eventStream = "event_stream"

	// Create payload transformer
	s.payloadTransformer = strategyJSON.NewPayloadTransformer()

	// Use a persistence strategy
	persistenceStrategy, err := strategyPostgres.NewSingleStreamStrategy(s.payloadTransformer)
	s.Require().NoError(err, "failed initializing persistent strategy")

	// Create message factory
	messageFactory, err := strategySQL.NewAggregateChangedFactory(s.payloadTransformer)
	s.Require().NoError(err, "failed on dependencies load")

	// Create event store
	s.eventStore, err = postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	s.Require().NoError(err, "failed on dependencies load")

	// Create the event stream
	ctx := context.Background()
	err = s.eventStore.Create(ctx, s.eventStream)
	s.Require().NoError(err, "failed on create event stream")

	// Set up the projection tables etc.
	s.eventStoreTable, err = persistenceStrategy.GenerateTableName(s.eventStream)
	s.Require().NoError(err, "failed to generate eventstream table name")
}

func (s *projectorSuite) TearDownTest() {
	s.eventStore = nil
	s.eventStream = ""
	s.eventStoreTable = ""
	s.payloadTransformer = nil

	s.PostgresSuite.TearDownTest()
}

func (s *projectorSuite) appendEvents(aggregateID aggregate.ID, events []interface{}) {
	ctx := context.Background()

	// Find the last event
	matcher := metadata.WithConstraint(
		metadata.WithConstraint(
			metadata.NewMatcher(),
			aggregate.TypeKey,
			metadata.Equals,
			accountAggregateTypeName,
		),
		aggregate.IDKey,
		metadata.Equals,
		aggregateID,
	)
	stream, err := s.eventStore.Load(ctx, s.eventStream, 0, nil, matcher)
	s.Require().NoError(err)

	var lastVersion int
	for stream.Next() {
		msg, _, err := stream.Message()
		s.Require().NoError(err)

		lastVersion = int(msg.Metadata().Value(aggregate.VersionKey).(float64))
	}
	s.Require().NoError(stream.Err())

	// Transform the events into messages
	messages := make([]goengine.Message, len(events))
	for i, event := range events {
		m := metadata.WithValue(
			metadata.WithValue(
				metadata.WithValue(metadata.New(), aggregate.IDKey, aggregateID),
				aggregate.VersionKey,
				lastVersion+i+1,
			),
			aggregate.TypeKey,
			"account",
		)

		message, err := aggregate.ReconstituteChange(
			aggregateID,
			goengine.GenerateUUID(),
			event,
			m,
			time.Now().UTC(),
			uint(i+1),
		)
		s.Require().NoError(err, "failed on create messages")

		messages[i] = message
	}

	// Append the messages to the stream
	err = s.eventStore.AppendTo(ctx, s.eventStream, messages)
	s.Require().NoError(err, "failed to append messages")
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func (s *projectorSuite) waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
