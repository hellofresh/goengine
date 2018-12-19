// +build integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore"
	eventStoreJSON "github.com/hellofresh/goengine/eventstore/json"
	"github.com/hellofresh/goengine/eventstore/postgres"
	eventStoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/internal/test"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	streamProjectorTestSuite struct {
		test.PostgresSuite

		eventStream        eventstore.StreamName
		eventStore         *postgres.EventStore
		payloadTransformer *eventStoreJSON.PayloadTransformer
	}
)

func TestStreamProjectorSuite(t *testing.T) {
	suite.Run(t, new(streamProjectorTestSuite))
}

func (s *streamProjectorTestSuite) SetupTest() {
	s.PostgresSuite.SetupTest()
	db := s.DB()

	s.eventStream = eventstore.StreamName("event_stream")

	// Create payload transformer
	s.payloadTransformer = eventStoreJSON.NewPayloadTransformer()

	// Use a persistence strategy
	persistenceStrategy, err := postgres.NewPostgresStrategy(s.payloadTransformer)
	s.Require().NoError(err, "failed initializing persistent strategy")

	// Create message factory
	messageFactory, err := eventStoreSQL.NewAggregateChangedFactory(s.payloadTransformer)
	s.Require().NoError(err, "failed on dependencies load")

	// Create event store
	s.eventStore, err = postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	s.Require().NoError(err, "failed on dependencies load")

	// Create the event stream
	ctx := context.Background()
	err = s.eventStore.Create(ctx, s.eventStream)
	s.Require().NoError(err, "failed on create event stream")

	// Setup the projection tables etc.
	eventStreamTable, err := persistenceStrategy.GenerateTableName(s.eventStream)
	s.Require().NoError(err, "failed to generate eventstream table name")

	queries := postgres.StreamProjectorCreateSchema("projections", s.eventStream, eventStreamTable)
	for _, query := range queries {
		_, err := db.ExecContext(ctx, query)
		s.Require().NoError(err, "failed to create projection tables etc.")
	}
}

func (s *streamProjectorTestSuite) TearDownTest() {
	s.eventStore = nil
	s.eventStream = ""
	s.payloadTransformer = nil

	s.PostgresSuite.TearDownTest()
}

func (s *streamProjectorTestSuite) TestRun() {
	var wg sync.WaitGroup
	defer func() {
		if s.waitTimeout(&wg, 5*time.Second) {
			s.T().Fatal("projection.Run in go routines failed to return")
		}
	}()

	s.Require().NoError(
		s.payloadTransformer.RegisterPayload("account_debited", func() interface{} {
			return AccountDeposited{}
		}),
	)
	s.Require().NoError(
		s.payloadTransformer.RegisterPayload("account_credited", func() interface{} {
			return AccountCredited{}
		}),
	)

	projectorCtx, projectorCancel := context.WithCancel(context.Background())
	defer projectorCancel()

	projector, err := postgres.NewStreamProjector(
		s.PostgresDSN,
		s.eventStore,
		s.payloadTransformer,
		&DepositedProjection{},
		"projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	// Run the projector in the background
	wg.Add(1)
	go func() {
		if err := projector.Run(projectorCtx, true); err != nil {
			assert.NoError(s.T(), err, "projector.Run returned an error")
		}
		wg.Done()
	}()

	// Be evil and start run the projection again to ensure mutex is used and the context is respected
	wg.Add(1)
	go func() {
		if err := projector.Run(projectorCtx, true); err != nil {
			assert.NoError(s.T(), err, "projector.Run returned an error")
		}
		wg.Done()
	}()

	// Let the go routines start
	runtime.Gosched()

	// Add events to the event stream
	aggregateIds := []aggregate.ID{
		aggregate.GenerateID(),
	}
	s.appendEvents(map[aggregate.ID][]interface{}{
		aggregateIds[0]: {
			AccountDeposited{Amount: 100},
			AccountCredited{Amount: 50},
			AccountDeposited{Amount: 10},
			AccountDeposited{Amount: 5},
			AccountDeposited{Amount: 100},
			AccountDeposited{Amount: 1},
		},
	})
	s.expectProjectionState("deposited_report", 6, `{"Total": 5, "TotalAmount": 216}`)

	// Add events to the event stream
	s.appendEvents(map[aggregate.ID][]interface{}{
		aggregateIds[0]: {
			AccountDeposited{Amount: 100},
			AccountDeposited{Amount: 1},
		},
	})

	s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)

	projectorCancel()

	s.Run("projection should not rerun events", func() {
		projector, err := postgres.NewStreamProjector(
			s.PostgresDSN,
			s.eventStore,
			s.payloadTransformer,
			&DepositedProjection{},
			"projections",
			s.Logger,
		)
		s.Require().NoError(err, "failed to create projector")

		err = projector.Run(context.Background(), false)
		s.Require().NoError(err, "failed to run projector")

		s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)
	})
}

func (s *streamProjectorTestSuite) TestRun_Once() {
	s.Require().NoError(
		s.payloadTransformer.RegisterPayload("account_debited", func() interface{} {
			return AccountDeposited{}
		}),
	)
	s.Require().NoError(
		s.payloadTransformer.RegisterPayload("account_credited", func() interface{} {
			return AccountCredited{}
		}),
	)

	aggregateIds := []aggregate.ID{
		aggregate.GenerateID(),
	}
	// Add events to the event stream
	s.appendEvents(map[aggregate.ID][]interface{}{
		aggregateIds[0]: {
			AccountDeposited{Amount: 100},
			AccountCredited{Amount: 50},
			AccountDeposited{Amount: 10},
			AccountDeposited{Amount: 5},
			AccountDeposited{Amount: 100},
			AccountDeposited{Amount: 1},
		},
	})

	projector, err := postgres.NewStreamProjector(
		s.PostgresDSN,
		s.eventStore,
		s.payloadTransformer,
		&DepositedProjection{},
		"projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	s.Run("Run projections", func() {
		ctx := context.Background()

		err := projector.Run(ctx, false)
		s.Require().NoError(err)

		s.expectProjectionState("deposited_report", 6, `{"Total": 5, "TotalAmount": 216}`)

		s.Run("Run projection again", func() {
			// Append more events
			s.appendEvents(map[aggregate.ID][]interface{}{
				aggregateIds[0]: {
					AccountDeposited{Amount: 100},
					AccountDeposited{Amount: 1},
				},
			})

			err := projector.Run(ctx, false)
			s.Require().NoError(err)

			s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)
		})
	})
}

func (s *streamProjectorTestSuite) TestDelete() {
	var projectionExists bool

	projection := &DepositedProjection{}
	projector, err := postgres.NewStreamProjector(
		s.PostgresDSN,
		s.eventStore,
		s.payloadTransformer,
		projection,
		"projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	// Run the projection to ensure it exists
	err = projector.Run(context.Background(), false)
	s.Require().NoError(err, "failed to run projector")

	row := s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM projections WHERE name = $1)`, projection.Name())
	s.Require().NoError(row.Scan(&projectionExists))
	s.Require().True(projectionExists, "projector.Run failed to create projection entry")

	// Remove projection
	err = projector.Delete(context.Background())
	s.Require().NoError(err)

	row = s.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM projections WHERE name = $1)`, projection.Name())
	s.Require().NoError(row.Scan(&projectionExists))
	s.Require().False(projectionExists)
}

func (s *streamProjectorTestSuite) expectProjectionState(name string, expectedPosition int64, expectedState string) {
	stmt, err := s.DB().Prepare(`SELECT position, state FROM projections WHERE name = $1`)
	s.Require().NoError(err)

	var (
		position int64
		state    string
	)

	for i := 0; i < 20; i++ {
		res := stmt.QueryRow(name)
		if err := res.Scan(&position, &state); err != nil {
			if err == sql.ErrNoRows {
				continue
			}

			s.Require().NoError(err)
			return
		}

		if position >= expectedPosition {
			s.Equal(expectedPosition, position)
			s.JSONEq(expectedState, state)
			return
		}

		// The expected state was not found to wait for a bit to allow the projector go routine/process to catch up
		time.Sleep(50 * time.Millisecond)
	}

	s.Require().Equal(expectedPosition, position, "failed to fetch expected projection state")
}

func (s *streamProjectorTestSuite) appendEvents(events map[aggregate.ID][]interface{}) {
	ctx := context.Background()
	for aggID, aggEvents := range events {
		// Find the last event
		matcher := metadata.WithConstraint(
			metadata.WithConstraint(
				metadata.NewMatcher(),
				aggregate.TypeKey,
				metadata.Equals,
				"account",
			),
			aggregate.IDKey,
			metadata.Equals,
			aggID,
		)
		stream, err := s.eventStore.Load(ctx, s.eventStream, 0, nil, matcher)
		s.Require().NoError(err)

		var lastVersion int
		for stream.Next() {
			msg, _, err := stream.Message()
			s.Require().NoError(err)

			lastVersion = int(msg.Metadata().Value(aggregate.VersionKey).(float64))
		}

		// Transform the events into messages
		messages := make([]messaging.Message, len(aggEvents))
		for i, event := range aggEvents {
			m := metadata.WithValue(
				metadata.WithValue(
					metadata.WithValue(metadata.New(), aggregate.IDKey, aggID),
					aggregate.VersionKey,
					lastVersion+i+1,
				),
				aggregate.TypeKey,
				"account",
			)

			message, err := aggregate.ReconstituteChange(
				aggID,
				messaging.GenerateUUID(),
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
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func (s *streamProjectorTestSuite) waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
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

var (
	_ eventstore.Projection = &DepositedProjection{}
)

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

func (p *DepositedProjection) Init(ctx context.Context) (interface{}, error) {
	return depositedProjectionState{}, nil
}

func (p *DepositedProjection) Name() string {
	return "deposited_report"
}

func (p *DepositedProjection) FromStream() eventstore.StreamName {
	return "event_stream"
}

func (p *DepositedProjection) Handlers() map[string]eventstore.ProjectionHandler {
	return map[string]eventstore.ProjectionHandler{
		"account_debited": func(ctx context.Context, state interface{}, message messaging.Message) (interface{}, error) {
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

func (p *DepositedProjection) ReconstituteState(data []byte) (interface{}, error) {
	var state depositedProjectionState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return state, nil
}

func (p *DepositedProjection) Reset(ctx context.Context) error {
	return nil
}

func (p *DepositedProjection) Delete(ctx context.Context) error {
	return nil
}
