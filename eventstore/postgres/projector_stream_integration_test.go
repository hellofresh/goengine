// +build integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
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
)

func TestStreamProjector_Run(t *testing.T) {
	dbDSN, exists := os.LookupEnv("POSTGRES_DSN")
	if !exists {
		t.Fatalf("missing POSTGRES_DSN enviroment variable")
	}
	test.PostgresDatabase(t, func(db *sql.DB) {
		eventStream, store, transformer := setupEventStoreAndProjections(t, db)

		transformer.RegisterPayload("account_debited", func() interface{} {
			return AccountDeposited{}
		})
		transformer.RegisterPayload("account_credited", func() interface{} {
			return AccountCredited{}
		})

		projectorCtx, projectorCancel := context.WithCancel(context.Background())
		defer projectorCancel()

		projector, err := postgres.NewStreamProjector(
			dbDSN,
			store,
			transformer,
			&DepositedProjection{},
			"projections",
			nil,
		)
		if err != nil {
			t.Fatalf("failed to create projector %s", err)
		}

		// Run the projector in the background
		go func() {
			err := projector.Run(projectorCtx, true)
			if err != nil {
				t.Fatalf("projector.Run returned an error. %s", err)
			}
		}()

		// Be evil and start run the projection again to ensure mutex is used and the context is respected
		go func() {
			err := projector.Run(projectorCtx, true)
			if err != nil {
				t.Fatalf("projector.Run returned an error. %s", err)
			}
		}()
		// Sleep for a while so the projector can initialize
		time.Sleep(100 * time.Millisecond)

		// Add events to the event stream
		aggregateIds := []aggregate.ID{
			aggregate.GenerateID(),
		}
		appendEvents(t, store, eventStream,
			map[aggregate.ID][]interface{}{
				aggregateIds[0]: {
					AccountDeposited{Amount: 100},
					AccountCredited{Amount: 50},
					AccountDeposited{Amount: 10},
					AccountDeposited{Amount: 5},
					AccountDeposited{Amount: 100},
					AccountDeposited{Amount: 1},
				},
			},
		)

		expectProjectionState(
			t,
			db,
			"deposited_report",
			6,
			`{"Total": 5, "TotalAmount": 216}`,
		)

		// Add events to the event stream
		appendEvents(t, store, eventStream,
			map[aggregate.ID][]interface{}{
				aggregateIds[0]: {
					AccountDeposited{Amount: 100},
					AccountDeposited{Amount: 1},
				},
			},
		)

		expectProjectionState(
			t,
			db,
			"deposited_report",
			8,
			`{"Total": 7, "TotalAmount": 317}`,
		)

		projectorCancel()

		t.Run("projection should not rerun events", func(t *testing.T) {
			projector, err := postgres.NewStreamProjector(
				dbDSN,
				store,
				transformer,
				&DepositedProjection{},
				"projections",
				nil,
			)
			if err != nil {
				t.Fatalf("failed to create projector %s", err)
			}

			err = projector.Run(context.Background(), false)
			if assert.NoError(t, err) {
				expectProjectionState(
					t,
					db,
					"deposited_report",
					8,
					`{"Total": 7, "TotalAmount": 317}`,
				)
			}
		})
	})
}

func TestStreamProjector_Run_Once(t *testing.T) {
	dbDSN, exists := os.LookupEnv("POSTGRES_DSN")
	if !exists {
		t.Fatalf("missing POSTGRES_DSN enviroment variable")
	}

	test.PostgresDatabase(t, func(db *sql.DB) {
		eventStream, store, transformer := setupEventStoreAndProjections(t, db)

		transformer.RegisterPayload("account_debited", func() interface{} {
			return AccountDeposited{}
		})
		transformer.RegisterPayload("account_credited", func() interface{} {
			return AccountCredited{}
		})

		aggregateIds := []aggregate.ID{
			aggregate.GenerateID(),
		}
		// Add events to the event stream
		appendEvents(t, store, eventStream,
			map[aggregate.ID][]interface{}{
				aggregateIds[0]: {
					AccountDeposited{Amount: 100},
					AccountCredited{Amount: 50},
					AccountDeposited{Amount: 10},
					AccountDeposited{Amount: 5},
					AccountDeposited{Amount: 100},
					AccountDeposited{Amount: 1},
				},
			},
		)

		projector, err := postgres.NewStreamProjector(
			dbDSN,
			store,
			transformer,
			&DepositedProjection{},
			"projections",
			nil,
		)
		if err != nil {
			t.Fatalf("failed to create projector %s", err)
		}

		t.Run("Run projections", func(t *testing.T) {
			asserts := assert.New(t)
			ctx := context.Background()

			err := projector.Run(ctx, false)
			if !asserts.NoError(err) {
				t.Fail()
			}

			expectProjectionState(
				t,
				db,
				"deposited_report",
				6,
				`{"Total": 5, "TotalAmount": 216}`,
			)

			t.Run("Run projection again", func(t *testing.T) {
				// Append more events
				appendEvents(t, store, eventStream,
					map[aggregate.ID][]interface{}{
						aggregateIds[0]: {
							AccountDeposited{Amount: 100},
							AccountDeposited{Amount: 1},
						},
					},
				)

				err := projector.Run(ctx, false)
				if !asserts.NoError(err) {
					t.Fail()
				}

				expectProjectionState(
					t,
					db,
					"deposited_report",
					8,
					`{"Total": 7, "TotalAmount": 317}`,
				)
			})
		})
	})
}

func TestStreamProjector_Delete(t *testing.T) {
	dbDSN, exists := os.LookupEnv("POSTGRES_DSN")
	if !exists {
		t.Fatalf("missing POSTGRES_DSN enviroment variable")
	}

	test.PostgresDatabase(t, func(db *sql.DB) {
		var projectionExists bool
		asserts := assert.New(t)

		_, store, transformer := setupEventStoreAndProjections(t, db)

		projection := &DepositedProjection{}
		projector, err := postgres.NewStreamProjector(
			dbDSN,
			store,
			transformer,
			projection,
			"projections",
			nil,
		)
		if err != nil {
			t.Fatalf("failed to create projector %s", err)
		}

		// Run the projection to ensure it exists
		if err := projector.Run(context.Background(), false); err != nil {
			t.Fatalf("failed to run projector %s", err)
		}
		row := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM projections WHERE name = $1)`, projection.Name())
		if !asserts.NoError(row.Scan(&projectionExists)) || !projectionExists {
			t.Fatal("projector.Run failed to create projection entry")
		}

		// Remove projection
		err = projector.Delete(context.Background())
		if !asserts.NoError(err) {
			return
		}

		row = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM projections WHERE name = $1)`, projection.Name())
		if asserts.NoError(row.Scan(&projectionExists)) {
			asserts.False(projectionExists)
		}
	})
}

func setupEventStoreAndProjections(t *testing.T, db *sql.DB) (eventstore.StreamName, *postgres.EventStore, *eventStoreJSON.PayloadTransformer) {
	eventStream := eventstore.StreamName("event_stream")

	// Create payload transformer
	transformer := eventStoreJSON.NewPayloadTransformer()

	// Use a persistence strategy
	persistenceStrategy, err := postgres.NewPostgresStrategy(transformer)
	if err != nil {
		t.Fatalf("failed initializing persistent strategy %s", err)
	}

	// Create message factory
	messageFactory, err := eventStoreSQL.NewAggregateChangedFactory(transformer)
	if err != nil {
		t.Fatalf("failed on dependencies load %s", err)
	}

	// Create event store
	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	if err != nil {
		t.Fatalf("failed on dependencies load %s", err)
	}

	// Create the event stream
	ctx := context.Background()
	if err := store.Create(ctx, eventStream); err != nil {
		t.Fatalf("failed on create event stream %s", err)
	}

	// Setup the projection tables etc.
	eventStreamTable, err := persistenceStrategy.GenerateTableName(eventStream)
	if err != nil {
		t.Fatalf("failed to generate eventstream table name %s", err)
	}
	queries := postgres.StreamProjectorCreateSchema("projections", eventStream, eventStreamTable)
	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatalf("failed to create projection tables etc. %s", err)
		}
	}

	return eventStream, store, transformer
}

func expectProjectionState(t *testing.T, db *sql.DB, name string, expectedPosition int64, expectedState string) {
	asserts := assert.New(t)

	stmt, err := db.Prepare(`SELECT position, state FROM projections WHERE name = $1`)
	if err != nil {
		t.Fatal(err)
		return
	}

	var (
		position int64
		state    string
	)

	for i := 0; i < 10; i++ {
		res := stmt.QueryRow(name)
		if err := res.Scan(&position, &state); err != nil {
			if err == sql.ErrNoRows {
				continue
			}

			asserts.NoError(err)
			return
		}

		if position >= expectedPosition {
			asserts.Equal(expectedPosition, position)
			asserts.JSONEq(expectedState, state)
			return
		}

		// The expected state was not found to wait for a bit to allow the projector go routine/process to catch up
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("failed to fetch expected projection state (expected %d got %d)", expectedPosition, position)
}

func appendEvents(t *testing.T, store *postgres.EventStore, streamName eventstore.StreamName, events map[aggregate.ID][]interface{}) {
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
		stream, err := store.Load(ctx, streamName, 0, nil, matcher)
		if !assert.NoError(t, err) {
			t.Fail()
		}

		var lastVersion int
		for stream.Next() {
			msg, _, err := stream.Message()
			if !assert.NoError(t, err) {
				t.Fail()
			}

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
			if err != nil {
				t.Fatalf("failed on create messages %s", err)
			}

			messages[i] = message
		}

		// Append the messages to the stream
		if err := store.AppendTo(ctx, streamName, messages); !assert.NoError(t, err) {
			t.Fail()
		}
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
