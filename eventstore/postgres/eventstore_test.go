// +build unit

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/postgres"
	eventstoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/internal/test"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewEventStore(t *testing.T) {
	test.RunWithMockDB(t, "invalid arguments", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		testCases := []struct {
			title       string
			strategy    eventstore.PersistenceStrategy
			db          *sql.DB
			factory     eventstoreSQL.MessageFactory
			expectedErr error
		}{
			{
				"No persistence strategy",
				nil,
				db,
				&mocks.MessageFactory{},
				postgres.ErrNoAggregateStreamStrategy,
			},
			{
				"No database",
				&mocks.PersistenceStrategy{},
				nil,
				&mocks.MessageFactory{},
				postgres.ErrNoDBConnect,
			},
			{
				"No message factory",
				&mocks.PersistenceStrategy{},
				db,
				nil,
				postgres.ErrNoMessageFactory,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				store, err := postgres.NewEventStore(testCase.strategy, testCase.db, testCase.factory, nil)

				asserts := assert.New(t)
				if asserts.Error(err) {
					asserts.Equal(testCase.expectedErr, err)
				}
				asserts.Nil(store)
			})
		}
	})
}

func TestEventStore_Create(t *testing.T) {
	test.RunWithMockDB(t, "Check create table with indexes", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		mockHasStreamQuery(false, dbMock)
		dbMock.ExpectBegin()
		dbMock.ExpectExec(`CREATE TABLE "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectExec(`CREATE UNIQUE INDEX ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectExec(`CREATE INDEX ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectCommit()

		store := createEventStore(t, db, &mocks.PayloadConverter{})

		err := store.Create(context.Background(), "orders")
		assert.NoError(t, err)
	})

	test.RunWithMockDB(t, "Check transaction rollback", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		expectedError := errors.New("index error")

		mockHasStreamQuery(false, dbMock)
		dbMock.ExpectBegin()
		dbMock.ExpectExec(`CREATE TABLE "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectExec(`CREATE UNIQUE INDEX(.+)ON "events_orders"(.+)`).WillReturnError(expectedError)
		dbMock.ExpectRollback()

		store := createEventStore(t, db, &mocks.PayloadConverter{})

		err := store.Create(context.Background(), "orders")
		if assert.Error(t, err) {
			assert.Equal(t, expectedError, err)
		}
	})
	test.RunWithMockDB(t, "Empty stream name", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		store := createEventStore(t, db, &mocks.PayloadConverter{})

		err := store.Create(context.Background(), "")
		if assert.Error(t, err) {
			assert.Equal(t, postgres.ErrEmptyStreamName, err)
		}
	})

	test.RunWithMockDB(t, "Stream table already exist", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		mockHasStreamQuery(true, dbMock)

		store := createEventStore(t, db, &mocks.PayloadConverter{})

		err := store.Create(context.Background(), "orders")
		if assert.Error(t, err) {
			assert.Equal(t, postgres.ErrTableAlreadyExists, err)
		}
	})

	test.RunWithMockDB(t, "No queries in strategy", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		asserts := assert.New(t)

		strategy := &mocks.PersistenceStrategy{}
		strategy.On("ColumnNames").Return([]string{})
		strategy.On("GenerateTableName", goengine_dev.StreamName("orders")).Return("events_orders", nil)
		strategy.On("CreateSchema", "events_orders").Return([]string{})

		store, err := postgres.NewEventStore(strategy, db, &mocks.MessageFactory{}, nil)
		if !asserts.NoError(err) {
			t.Fail()
		}

		err = store.Create(context.Background(), "orders")
		if asserts.Error(err) {
			asserts.Equal(postgres.ErrNoCreateTableQueries, err)
		}
	})
}

func TestEventStore_HasStream(t *testing.T) {
	testCases := []struct {
		title      string
		streamName goengine_dev.StreamName
		sqlResult  bool
		expected   bool
	}{
		{
			"Stream does not exist",
			"orders",
			false,
			false,
		},
		{
			"Stream exists",
			"orders",
			true,
			true,
		},
		{
			"Empty stream",
			"",
			false,
			false,
		},
	}

	for _, testCase := range testCases {
		test.RunWithMockDB(t, testCase.title, func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
			mockHasStreamQuery(testCase.sqlResult, dbMock)

			store := createEventStore(t, db, &mocks.PayloadConverter{})

			exists := store.HasStream(context.Background(), testCase.streamName)
			assert.Equal(t, testCase.expected, exists)
		})
	}
}

func TestEventStore_AppendTo(t *testing.T) {
	test.RunWithMockDB(t, "Insert successfully", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		payloadConverter, messages := mockMessages()

		dbMock.ExpectExec(`INSERT(.+)VALUES \(\$1,\$2,\$3,\$4,\$5\),\(\$6,\$7,\$8,\$9,\$10\),\(\$11(.+)`).
			WillReturnResult(sqlmock.NewResult(111, 3))

		eventStore := createEventStore(t, db, payloadConverter)

		err := eventStore.AppendTo(context.Background(), "orders", messages)
		assert.NoError(t, err)
	})

	test.RunWithMockDB(t, "Empty stream name", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		messages := []goengine_dev.Message{
			mockMessage(
				goengine_dev.GenerateUUID(),
				[]byte(`{"Name":"alice","Balance":0}`),
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		eventStore := createEventStore(t, db, &mocks.PayloadConverter{})

		err := eventStore.AppendTo(context.Background(), "", messages)
		if assert.Error(t, err) {
			assert.Equal(t, postgres.ErrEmptyStreamName, err)
		}
	})

	test.RunWithMockDB(t, "Prepare data error", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		expectedError := errors.New("prepare data expected error")

		messages := []goengine_dev.Message{
			mockMessage(
				goengine_dev.GenerateUUID(),
				[]byte(`{"Name":"alice","Balance":0}`),
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		persistenceStrategy := &mocks.PersistenceStrategy{}
		persistenceStrategy.On("PrepareData", messages).Return(nil, expectedError)
		persistenceStrategy.On("GenerateTableName", goengine_dev.StreamName("orders")).Return("events_orders", nil)
		persistenceStrategy.On("ColumnNames").Return([]string{"event_id", "event_name"})

		store, err := postgres.NewEventStore(persistenceStrategy, db, &mocks.MessageFactory{}, nil)
		asserts := assert.New(t)
		if !asserts.NoError(err) {
			t.Fail()
		}

		err = store.AppendTo(context.Background(), "orders", messages)
		if asserts.Error(err) {
			asserts.Equal(expectedError, err)
		}
	})
}

func TestEventStore_Load(t *testing.T) {
	t.Run("Load events", func(t *testing.T) {
		columns := []string{"no", "payload", "metadata"}
		var limit50 uint = 50

		testCases := []struct {
			title         string
			fromNumber    int64
			count         *uint
			matcher       func() metadata.Matcher
			expectedQuery string
		}{
			{
				"With matcher",
				1,
				nil,
				func() metadata.Matcher {
					m := metadata.NewMatcher()
					m = metadata.WithConstraint(m, "version", metadata.GreaterThan, 1)
					m = metadata.WithConstraint(m, "version", metadata.LowerThan, 100)
					return m
				},
				`SELECT \* FROM event_stream WHERE metadata ->> 'version' > \$1 AND metadata ->> 'version' < \$2 AND no >= \$3 ORDER BY no`,
			},
			{
				"Without matcher",
				1,
				nil,
				func() metadata.Matcher {
					return nil
				},
				`SELECT \* FROM event_stream WHERE no >= \$1 ORDER BY no`,
			},
			{
				"With limit",
				1,
				&limit50,
				func() metadata.Matcher {
					return nil
				},
				`SELECT \* FROM event_stream WHERE no >= \$1 ORDER BY no LIMIT 50`,
			},
		}

		for _, testCase := range testCases {
			test.RunWithMockDB(t, testCase.title, func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
				expectedStream := &mocks.EventStream{}

				dbMock.ExpectQuery(testCase.expectedQuery).WillReturnRows(sqlmock.NewRows(columns))

				factory := &mocks.MessageFactory{}
				factory.On("CreateEventStream", mock.AnythingOfType("*sql.Rows")).Once().Return(expectedStream, nil)

				strategy := &mocks.PersistenceStrategy{}
				strategy.On("ColumnNames").Return(columns)
				strategy.On("GenerateTableName", goengine_dev.StreamName("event_stream")).Return("event_stream", nil)

				store, err := postgres.NewEventStore(strategy, db, factory, nil)

				stream, err := store.Load(
					context.Background(),
					"event_stream",
					testCase.fromNumber,
					testCase.count,
					testCase.matcher(),
				)

				if assert.NoError(t, err) {
					assert.Equal(t, expectedStream, stream)
				}
				factory.AssertExpectations(t)
				strategy.AssertExpectations(t)
			})
		}
	})

	t.Run("persistent strategy failures", func(t *testing.T) {
		columns := []string{"no", "payload"}

		testCases := []struct {
			title         string
			strategy      func() *mocks.PersistenceStrategy
			expectedError error
		}{
			{
				"Empty table name returned",
				func() *mocks.PersistenceStrategy {
					strategy := &mocks.PersistenceStrategy{}
					strategy.On("ColumnNames").Return(columns)
					strategy.On("GenerateTableName", goengine_dev.StreamName("event_stream")).Return("", nil)
					return strategy
				},
				postgres.ErrTableNameEmpty,
			},
			{
				"Empty table name returned",
				func() *mocks.PersistenceStrategy {
					strategy := &mocks.PersistenceStrategy{}
					strategy.On("ColumnNames").Return(columns)
					strategy.
						On("GenerateTableName", goengine_dev.StreamName("event_stream")).
						Return("", errors.New("failed gen"))
					return strategy
				},
				errors.New("failed gen"),
			},
		}

		for _, testCase := range testCases {
			test.RunWithMockDB(t, testCase.title, func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
				strategy := testCase.strategy()
				store, err := postgres.NewEventStore(strategy, db, &mocks.MessageFactory{}, nil)

				stream, err := store.Load(context.Background(), "event_stream", 1, nil, nil)
				if assert.Error(t, err) {
					assert.Equal(t, testCase.expectedError, err)
					assert.Nil(t, stream)
				}
				strategy.AssertExpectations(t)
			})
		}
	})
}

func mockHasStreamQuery(result bool, mock sqlmock.Sqlmock) {
	mockRows := sqlmock.NewRows([]string{"type"}).AddRow(result)
	mock.ExpectQuery(`SELECT EXISTS\((.+)`).WithArgs("events_orders").WillReturnRows(mockRows)
}

func mockMessages() (*mocks.PayloadConverter, []goengine_dev.Message) {
	pc := &mocks.PayloadConverter{}
	messages := make([]goengine_dev.Message, 3)

	for i := 0; i < len(messages); i++ {
		payload := []byte(fmt.Sprintf(`{"Name":"alice_%d","Balance":0}`, i))
		messages[i] = mockMessage(
			goengine_dev.GenerateUUID(),
			payload,
			metadata.FromMap(map[string]interface{}{
				"type":    fmt.Sprintf("m%d", i),
				"version": i + 1,
			}),
			time.Now(),
		)

		pc.On("ConvertPayload", payload).Return(fmt.Sprintf("Payload%d", i), payload, nil)
	}

	return pc, messages
}

func createEventStore(t *testing.T, db *sql.DB, converter eventstore.PayloadConverter) *postgres.EventStore {
	asserts := assert.New(t)

	persistenceStrategy, err := postgres.NewPostgresStrategy(converter)
	if !asserts.NoError(err) {
		t.Fail()
	}

	store, err := postgres.NewEventStore(persistenceStrategy, db, &mocks.MessageFactory{}, nil)
	if !asserts.NoError(err) {
		t.Fail()
	}

	return store
}
