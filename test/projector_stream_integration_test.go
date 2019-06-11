// +build integration

package test_test

import (
	"context"
	"database/sql"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/hellofresh/goengine"

	"github.com/hellofresh/goengine/aggregate"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/extension/pq"
	strategyPostgres "github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	streamProjectorTestSuite struct {
		projectorSuite

		createProjectionStorage func(
			eventStoreTable,
			projectionTable string,
			projectionStateSerialization driverSQL.ProjectionStateSerialization,
			logger goengine.Logger,
		) (driverSQL.StreamProjectorStorage, error)
	}
)

func TestStreamProjectorSuite(t *testing.T) {
	t.Run("AdvisoryLock", func(t *testing.T) {
		suite.Run(t, &streamProjectorTestSuite{
			createProjectionStorage: func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (storage driverSQL.StreamProjectorStorage, e error) {
				return postgres.NewAdvisoryLockStreamProjectionStorage(eventStoreTable, projectionTable, serialization, true, logger)
			},
		})
	})
	t.Run("AdvisoryLock without locked field", func(t *testing.T) {
		suite.Run(t, &streamProjectorTestSuite{
			createProjectionStorage: func(eventStoreTable, projectionTable string, serialization driverSQL.ProjectionStateSerialization, logger goengine.Logger) (storage driverSQL.StreamProjectorStorage, e error) {
				return postgres.NewAdvisoryLockStreamProjectionStorage(eventStoreTable, projectionTable, serialization, false, logger)
			},
		})
	})
}

func (s *streamProjectorTestSuite) SetupTest() {
	s.projectorSuite.SetupTest()

	ctx := context.Background()
	queries := strategyPostgres.StreamProjectorCreateSchema("projections", s.eventStream, s.eventStoreTable)
	for _, query := range queries {
		_, err := s.DB().ExecContext(ctx, query)
		s.Require().NoError(err, "failed to create projection tables etc.")
	}
}

func (s *streamProjectorTestSuite) TearDownTest() {
	s.eventStore = nil
	s.eventStream = ""
	s.payloadTransformer = nil

	s.PostgresSuite.TearDownTest()
}

func (s *streamProjectorTestSuite) TestRunAndListen() {
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

	projectorStorage, err := s.createProjectionStorage(projection.Name(), "projections", projection, s.GetLogger())
	s.Require().NoError(err, "failed to create projector storage")

	project, err := driverSQL.NewStreamProjector(
		s.DB(),
		driverSQL.StreamProjectionEventStreamLoader(s.eventStore, projection.FromStream()),
		s.payloadTransformer,
		projection,
		projectorStorage,
		func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
		s.GetLogger(),
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
	aggregateIds := []aggregate.ID{
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
	s.expectProjectionState("deposited_report", 6, `{"Total": 5, "TotalAmount": 216}`)

	// Add events to the event stream
	s.appendEvents(aggregateIds[0], []interface{}{
		AccountDeposited{Amount: 100},
		AccountDeposited{Amount: 1},
	})

	s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)

	projectorCancel()

	s.Run("projection should not rerun events", func() {
		projection := &DepositedProjection{}

		projectorStorage, err := s.createProjectionStorage(projection.Name(), "projections", projection, s.GetLogger())
		s.Require().NoError(err, "failed to create projector storage")

		project, err := driverSQL.NewStreamProjector(
			s.DB(),
			driverSQL.StreamProjectionEventStreamLoader(s.eventStore, projection.FromStream()),
			s.payloadTransformer,
			projection,
			projectorStorage,
			func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
				return driverSQL.ProjectionFail
			},
			s.GetLogger(),
		)
		s.Require().NoError(err, "failed to create projector")

		err = project.Run(context.Background())
		s.Require().NoError(err, "failed to run projector")

		s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)
	})

	s.AssertNoLogsWithLevelOrHigher(logrus.ErrorLevel)
}

func (s *streamProjectorTestSuite) TestRun() {
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
	s.appendEvents(aggregateIds[0], []interface{}{
		AccountDeposited{Amount: 100},
		AccountCredited{Amount: 50},
		AccountDeposited{Amount: 10},
		AccountDeposited{Amount: 5},
		AccountDeposited{Amount: 100},
		AccountDeposited{Amount: 1},
	})

	projection := &DepositedProjection{}

	projectorStorage, err := s.createProjectionStorage(projection.Name(), "projections", projection, s.GetLogger())
	s.Require().NoError(err, "failed to create projector storage")

	project, err := driverSQL.NewStreamProjector(
		s.DB(),
		driverSQL.StreamProjectionEventStreamLoader(s.eventStore, projection.FromStream()),
		s.payloadTransformer,
		projection,
		projectorStorage,
		func(error, *driverSQL.ProjectionNotification) driverSQL.ProjectionErrorAction {
			return driverSQL.ProjectionFail
		},
		s.GetLogger(),
	)
	s.Require().NoError(err, "failed to create projector")

	s.Run("Run projections", func() {
		ctx := context.Background()

		err := project.Run(ctx)
		s.Require().NoError(err)

		s.expectProjectionState("deposited_report", 6, `{"Total": 5, "TotalAmount": 216}`)

		s.Run("Run projection again", func() {
			// Append more events
			s.appendEvents(aggregateIds[0], []interface{}{
				AccountDeposited{Amount: 100},
				AccountDeposited{Amount: 1},
			})

			err := project.Run(ctx)
			s.Require().NoError(err)

			s.expectProjectionState("deposited_report", 8, `{"Total": 7, "TotalAmount": 317}`)
		})
	})

	s.AssertNoLogsWithLevelOrHigher(logrus.ErrorLevel)
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
