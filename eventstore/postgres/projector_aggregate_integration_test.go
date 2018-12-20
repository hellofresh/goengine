// +build integration

package postgres_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	aggregateProjectorTestSuite struct {
		projectorSuite
	}

	projectionInfo struct {
		position int64
		state    string
	}
)

func TestAggregateProjectorSuite(t *testing.T) {
	suite.Run(t, new(aggregateProjectorTestSuite))
}

func (s *aggregateProjectorTestSuite) SetupTest() {
	s.projectorSuite.SetupTest()

	ctx := context.Background()
	queries := postgres.AggregateProjectorCreateSchema("agg_projections", s.eventStream, s.eventStoreTable)
	for _, query := range queries {
		_, err := s.DB().ExecContext(ctx, query)
		s.Require().NoError(err, "failed to create projection tables etc.")
	}

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
}

func (s *aggregateProjectorTestSuite) TestRun() {
	var wg sync.WaitGroup
	defer func() {
		if s.waitTimeout(&wg, 5*time.Second) {
			s.T().Fatal("projection.Run in go routines failed to return")
		}
	}()

	projectorCtx, projectorCancel := context.WithCancel(context.Background())
	defer projectorCancel()

	projector, err := postgres.NewAggregateProjector(
		s.PostgresDSN,
		s.eventStore,
		s.eventStoreTable,
		s.payloadTransformer,
		accountAggregateTypeName,
		&DepositedProjection{},
		"agg_projections",
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
		aggregate.GenerateID(),
	}
	s.appendEvents(aggregateIds[0], []interface{}{
		AccountDeposited{Amount: 100},
		AccountCredited{Amount: 50},
		AccountDeposited{Amount: 10},
		AccountDeposited{Amount: 5},
		AccountDeposited{Amount: 100},
		AccountDeposited{Amount: 1},
	})
	s.appendEvents(aggregateIds[1], []interface{}{
		AccountDeposited{Amount: 1},
	})

	s.assertAggregateProjectionStates(map[aggregate.ID]projectionInfo{
		aggregateIds[0]: {
			position: 6,
			state:    `{"Total": 5, "TotalAmount": 216}`,
		},
		aggregateIds[1]: {
			position: 7,
			state:    `{"Total": 1, "TotalAmount": 1}`,
		},
	})

	// Add events to the event stream
	s.appendEvents(aggregateIds[0], []interface{}{
		AccountDeposited{Amount: 100},
		AccountDeposited{Amount: 1},
	})
	s.assertAggregateProjectionStates(map[aggregate.ID]projectionInfo{
		aggregateIds[0]: {
			position: 9,
			state:    `{"Total": 7, "TotalAmount": 317}`,
		},
		aggregateIds[1]: {
			position: 7,
			state:    `{"Total": 1, "TotalAmount": 1}`,
		},
	})

	projectorCancel()

	s.Run("projection should not rerun events", func() {
		projector, err := postgres.NewAggregateProjector(
			s.PostgresDSN,
			s.eventStore,
			s.eventStoreTable,
			s.payloadTransformer,
			accountAggregateTypeName,
			&DepositedProjection{},
			"agg_projections",
			s.Logger,
		)
		s.Require().NoError(err, "failed to create projector")

		err = projector.Run(context.Background(), false)
		s.Require().NoError(err)

		s.assertAggregateProjectionStates(map[aggregate.ID]projectionInfo{
			aggregateIds[0]: {
				position: 9,
				state:    `{"Total": 7, "TotalAmount": 317}`,
			},
			aggregateIds[1]: {
				position: 7,
				state:    `{"Total": 1, "TotalAmount": 1}`,
			},
		})
	})
}

func (s *aggregateProjectorTestSuite) TestRun_Once() {
	aggregateIds := []aggregate.ID{
		aggregate.GenerateID(),
		aggregate.GenerateID(),
		aggregate.GenerateID(),
	}
	// Add events to the event stream
	s.appendEvents(aggregateIds[0], []interface{}{
		AccountDeposited{Amount: 100},
		AccountCredited{Amount: 50},
		AccountDeposited{Amount: 10},
		AccountDeposited{Amount: 5},
		AccountDeposited{Amount: 100},
		AccountDeposited{Amount: 1},
	})

	projector, err := postgres.NewAggregateProjector(
		s.PostgresDSN,
		s.eventStore,
		s.eventStoreTable,
		s.payloadTransformer,
		accountAggregateTypeName,
		&DepositedProjection{},
		"agg_projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	s.Run("Run projections", func() {
		ctx := context.Background()

		err := projector.Run(ctx, false)
		s.Require().NoError(err)

		s.assertAggregateProjectionStates(map[aggregate.ID]projectionInfo{
			aggregateIds[0]: {
				position: 6,
				state:    `{"Total": 5, "TotalAmount": 216}`,
			},
		})

		s.Run("Run projection again", func() {
			// Append more events
			s.appendEvents(aggregateIds[1], []interface{}{
				AccountDeposited{Amount: 100},
			})
			s.appendEvents(aggregateIds[2], []interface{}{
				AccountDeposited{Amount: 1},
				AccountDeposited{Amount: 100},
			})
			s.appendEvents(aggregateIds[0], []interface{}{
				AccountDeposited{Amount: 1},
			})

			err := projector.Run(ctx, false)
			s.Require().NoError(err)

			s.assertAggregateProjectionStates(map[aggregate.ID]projectionInfo{
				aggregateIds[0]: {
					position: 10,
					state:    `{"Total": 6, "TotalAmount": 217}`,
				},
				aggregateIds[1]: {
					position: 7,
					state:    `{"Total": 1, "TotalAmount": 100}`,
				},
				aggregateIds[2]: {
					position: 9,
					state:    `{"Total": 2, "TotalAmount": 101}`,
				},
			})
		})
	})
}

func (s *aggregateProjectorTestSuite) TestReset() {
	projection := &DepositedProjection{}
	projector, err := postgres.NewAggregateProjector(
		s.PostgresDSN,
		s.eventStore,
		s.eventStoreTable,
		s.payloadTransformer,
		accountAggregateTypeName,
		projection,
		"agg_projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	// Add events to the event stream
	s.appendEvents(aggregate.GenerateID(), []interface{}{
		AccountDeposited{Amount: 100},
	})
	s.appendEvents(aggregate.GenerateID(), []interface{}{
		AccountDeposited{Amount: 100},
	})
	s.appendEvents(aggregate.GenerateID(), []interface{}{
		AccountDeposited{Amount: 100},
	})

	// Run projector
	err = projector.Run(context.Background(), false)
	s.Require().NoError(err, "failed to run projector")

	// Check that the projection rows exist
	var rowCount int
	err = s.DB().QueryRow(`SELECT COUNT(*) FROM agg_projections`).Scan(&rowCount)
	s.Require().NoError(err)
	s.Require().Equal(3, rowCount)

	// Remove projection
	err = projector.Reset(context.Background())
	s.Require().NoError(err)

	err = s.DB().QueryRow(`SELECT COUNT(*) FROM agg_projections`).Scan(&rowCount)
	s.Require().NoError(err)
	s.Require().Equal(0, rowCount)
}

func (s *aggregateProjectorTestSuite) TestDelete() {
	projection := &DepositedProjection{}
	projector, err := postgres.NewAggregateProjector(
		s.PostgresDSN,
		s.eventStore,
		s.eventStoreTable,
		s.payloadTransformer,
		accountAggregateTypeName,
		projection,
		"agg_projections",
		s.Logger,
	)
	s.Require().NoError(err, "failed to create projector")

	// Ensure the projection table exists
	s.Require().True(s.DBTableExists("agg_projections"), "expected projection table to exist")

	// Remove projection
	err = projector.Delete(context.Background())
	s.Require().NoError(err)

	s.Require().False(s.DBTableExists("agg_projections"), "expected projection table to be removed")
}

func (s *aggregateProjectorTestSuite) assertAggregateProjectionStates(expectedProjections map[aggregate.ID]projectionInfo) {
	stmt, err := s.DB().Prepare(`SELECT aggregate_id, position, state FROM agg_projections`)
	if err != nil {
		s.T().Fatal(err)
		return
	}

	var result map[aggregate.ID]projectionInfo
	for i := 0; i < 10; i++ {
		rows, err := stmt.Query()
		s.Require().NoError(err)

		result = make(map[aggregate.ID]projectionInfo, len(expectedProjections))
		for rows.Next() {
			var (
				aggregateID aggregate.ID
				position    int64
				state       string
			)

			s.Require().NoError(rows.Scan(&aggregateID, &position, &state))

			result[aggregateID] = projectionInfo{
				position: position,
				state:    state,
			}
		}
		s.Require().NoError(rows.Err())

		if len(expectedProjections) == len(result) && assert.ObjectsAreEqual(expectedProjections, result) {
			return
		}

		// The expected state was not found to wait for a bit to allow the projector go routine/process to catch up
		time.Sleep(50 * time.Millisecond)
	}

	s.Require().Equal(expectedProjections, result, "failed to fetch expected projection state")
}
