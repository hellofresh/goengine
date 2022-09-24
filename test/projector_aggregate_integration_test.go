//go:build integration

package test_test

import (
	"context"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/aggregate"
	"github.com/hellofresh/goengine/v2/driver/sql"
	driverSQL "github.com/hellofresh/goengine/v2/driver/sql"
	"github.com/hellofresh/goengine/v2/driver/sql/postgres"
	"github.com/hellofresh/goengine/v2/extension/pq"
	strategyPostgres "github.com/hellofresh/goengine/v2/strategy/json/sql/postgres"
)

type (
	aggregateProjectorTestSuite struct {
		projectorSuite

		createProjectionStorage func(
			eventStoreTable,
			projectionTable string,
			projectionStateSerialization driverSQL.ProjectionStateSerialization,
			logger goengine.Logger,
		) (driverSQL.AggregateProjectorStorage, error)
	}

	projectionInfo struct {
		position int64
		state    string
	}
)

func TestAggregateProjectorSuite(t *testing.T) {
	t.Run("AdvisoryLock", func(t *testing.T) {
		suite.Run(t, &aggregateProjectorTestSuite{
			createProjectionStorage: func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (storage driverSQL.AggregateProjectorStorage, e error) {
				return postgres.NewAdvisoryLockAggregateProjectionStorage(eventStoreTable, projectionTable, serialization, true, logger)
			},
		})
	})
	t.Run("AdvisoryLock without locked field", func(t *testing.T) {
		suite.Run(t, &aggregateProjectorTestSuite{
			createProjectionStorage: func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (storage driverSQL.AggregateProjectorStorage, e error) {
				return postgres.NewAdvisoryLockAggregateProjectionStorage(eventStoreTable, projectionTable, serialization, false, logger)
			},
		})
	})
}

func (s *aggregateProjectorTestSuite) SetupTest() {
	s.projectorSuite.SetupTest()

	ctx := context.Background()
	queries := strategyPostgres.AggregateProjectorCreateSchema("agg_projections", s.eventStream, s.eventStoreTable)
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

func (s *aggregateProjectorTestSuite) TestRunAndListen() {
	var wg sync.WaitGroup

	projectorCtx, projectorCancel := context.WithCancel(context.Background())
	defer projectorCancel()

	projection := &DepositedProjection{}

	listener, err := pq.NewListener(
		s.PostgresDSN,
		string(projection.FromStream()),
		time.Millisecond,
		time.Second,
		s.GetLogger(),
		s.Metrics,
	)
	s.Require().NoError(err)

	projectorStorage, err := s.createProjectionStorage(s.eventStoreTable, "agg_projections", projection, s.GetLogger())
	s.Require().NoError(err, "failed to create projector storage")

	project, err := driverSQL.NewAggregateProjector(
		s.DB(),
		driverSQL.AggregateProjectionEventStreamLoader(s.eventStore, projection.FromStream(), accountAggregateTypeName),
		s.payloadTransformer,
		projection,
		projectorStorage,
		func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
		s.GetLogger(),
		s.Metrics,
		0,
	)
	s.Require().NoError(err, "failed to create projector")

	// Run the projector in the background
	wg.Add(1)
	go func() {
		if err := project.RunAndListen(projectorCtx, listener); err != nil {
			assert.NoError(s.T(), err, "project.Run returned an error")
		}
		wg.Done()
	}()

	// Be evil and start run the projection again to ensure mutex is used and the context is respected
	wg.Add(1)
	go func() {
		if err := project.RunAndListen(projectorCtx, listener); err != nil {
			assert.NoError(s.T(), err, "project.Run returned an error")
		}
		wg.Done()
	}()

	// Let the go routines start
	runtime.Gosched()

	// Ensure the projector is listening
	projectorIsListening, err := s.DBQueryIsRunningWithTimeout(regexp.MustCompile("LISTEN .*"), 5*time.Second)
	s.Require().NoError(err)
	s.Require().True(
		projectorIsListening,
		"expect projection to Listen for notifications",
	)

	// Add events to the event stream
	aggregateIds := createAggregateIds([]string{
		"3300b507-29cb-4899-a467-603b6409d0ce",
		"ce241bf3-2f8f-4e39-9a66-153bdca506fd",
	})
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
	if s.waitTimeout(&wg, 5*time.Second) {
		s.T().Fatal("projection.Run in go routines failed to return")
	}

	s.Run("projection should not rerun events", func() {
		projection := &DepositedProjection{}

		projectorStorage, err := s.createProjectionStorage(s.eventStoreTable, "agg_projections", projection, s.GetLogger())
		s.Require().NoError(err, "failed to create projector storage")

		project, err := driverSQL.NewAggregateProjector(
			s.DB(),
			driverSQL.AggregateProjectionEventStreamLoader(s.eventStore, projection.FromStream(), accountAggregateTypeName),
			s.payloadTransformer,
			projection,
			projectorStorage,
			func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
				return driverSQL.ProjectionFail
			},
			s.GetLogger(),
			s.Metrics,
			0,
		)
		s.Require().NoError(err, "failed to create projector")

		err = project.Run(context.Background())
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

	s.AssertNoLogsWithLevelOrHigher(logrus.ErrorLevel)
}

func (s *aggregateProjectorTestSuite) TestRun() {
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

	var err error

	projection := &DepositedProjection{}

	projectorStorage, err := s.createProjectionStorage(s.eventStoreTable, "agg_projections", projection, s.GetLogger())
	s.Require().NoError(err, "failed to create projector storage")

	project, err := driverSQL.NewAggregateProjector(
		s.DB(),
		driverSQL.AggregateProjectionEventStreamLoader(s.eventStore, projection.FromStream(), accountAggregateTypeName),
		s.payloadTransformer,
		projection,
		projectorStorage,
		func(error, *sql.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
		s.GetLogger(),
		s.Metrics,
		0,
	)
	s.Require().NoError(err, "failed to create projector")

	s.Run("Run projections", func() {
		ctx := context.Background()

		err := project.Run(ctx)
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

			err := project.Run(ctx)
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

	s.AssertNoLogsWithLevelOrHigher(logrus.ErrorLevel)
}

func (s *aggregateProjectorTestSuite) assertAggregateProjectionStates(expectedProjections map[aggregate.ID]projectionInfo) {
	stmt, err := s.DB().Prepare(`SELECT aggregate_id, position, state FROM agg_projections`)
	s.Require().NoError(err)

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

func createAggregateIds(ids []string) []aggregate.ID {
	var err error
	res := make([]aggregate.ID, len(ids))

	for i, id := range ids {
		res[i], err = aggregate.IDFromString(id)
		if err != nil {
			panic(err)
		}
	}

	return res
}
