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
	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/internal/test"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	mockSQL "github.com/hellofresh/goengine/mocks/driver/sql"
	strategyPostgres "github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventStore(t *testing.T) {
	test.RunWithMockDB(t, "invalid arguments", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		testCases := []struct {
			title                 string
			strategy              driverSQL.PersistenceStrategy
			db                    *sql.DB
			factory               driverSQL.MessageFactory
			expectedArgumentError string
		}{
			{
				"No persistence strategy",
				nil,
				db,
				&mockSQL.MessageFactory{},
				"persistenceStrategy",
			},
			{
				"No database",
				&mockSQL.PersistenceStrategy{},
				nil,
				&mockSQL.MessageFactory{},
				"db",
			},
			{
				"No message factory",
				&mockSQL.PersistenceStrategy{},
				db,
				nil,
				"messageFactory",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				store, err := postgres.NewEventStore(testCase.strategy, testCase.db, testCase.factory, nil)

				asserts := assert.New(t)
				if asserts.Error(err) {
					arg := err.(goengine.InvalidArgumentError)
					asserts.Equal(testCase.expectedArgumentError, string(arg))
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

		store := createEventStore(t, db, &mocks.MessagePayloadConverter{})

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

		store := createEventStore(t, db, &mocks.MessagePayloadConverter{})

		err := store.Create(context.Background(), "orders")
		assert.Equal(t, expectedError, err)
	})
	test.RunWithMockDB(t, "Empty stream name", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		store := createEventStore(t, db, &mocks.MessagePayloadConverter{})

		err := store.Create(context.Background(), "")
		if assert.Error(t, err) {
			arg := err.(goengine.InvalidArgumentError)
			assert.Equal(t, "streamName", string(arg))
		}
	})

	test.RunWithMockDB(t, "Stream table already exist", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		mockHasStreamQuery(true, dbMock)

		store := createEventStore(t, db, &mocks.MessagePayloadConverter{})

		err := store.Create(context.Background(), "orders")
		assert.Equal(t, postgres.ErrTableAlreadyExists, err)
	})

	test.RunWithMockDB(t, "No queries in strategy", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		asserts := assert.New(t)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		strategy := mockSQL.NewPersistenceStrategy(ctrl)
		strategy.EXPECT().ColumnNames().Return([]string{}).AnyTimes()
		strategy.EXPECT().EventColumnNames().Return([]string{}).AnyTimes()
		strategy.EXPECT().GenerateTableName(goengine.StreamName("orders")).Return("events_orders", nil).AnyTimes()
		strategy.EXPECT().CreateSchema("events_orders").Return([]string{}).AnyTimes()

		store, err := postgres.NewEventStore(strategy, db, &mockSQL.MessageFactory{}, nil)
		require.NoError(t, err)

		err = store.Create(context.Background(), "orders")
		asserts.Equal(postgres.ErrNoCreateTableQueries, err)
	})
}

func TestEventStore_HasStream(t *testing.T) {
	testCases := []struct {
		title      string
		streamName goengine.StreamName
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

			store := createEventStore(t, db, &mocks.MessagePayloadConverter{})

			exists := store.HasStream(context.Background(), testCase.streamName)
			assert.Equal(t, testCase.expected, exists)
		})
	}
}

func TestEventStore_AppendTo(t *testing.T) {
	test.RunWithMockDB(t, "Insert successfully", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		payloadConverter, messages := mockMessages(ctrl)

		dbMock.ExpectExec(`INSERT(.+)VALUES \(\$1,\$2,\$3,\$4,\$5\,\$6,\$7,\$8\),\(\$9,\$10,\$11,\$12,\$13,\$14,\$15,\$16\),\(\$17(.+)`).
			WillReturnResult(sqlmock.NewResult(111, 3))

		eventStore := createEventStore(t, db, payloadConverter)

		err := eventStore.AppendTo(context.Background(), "orders", messages)
		assert.NoError(t, err)
	})

	test.RunWithMockDB(t, "Empty stream name", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		messages := []goengine.Message{
			mocks.NewDummyMessage(
				goengine.GenerateUUID(),
				[]byte(`{"Name":"alice","Balance":0}`),
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		eventStore := createEventStore(t, db, &mocks.MessagePayloadConverter{})

		err := eventStore.AppendTo(context.Background(), "", messages)
		if assert.Error(t, err) {
			arg := err.(goengine.InvalidArgumentError)
			assert.Equal(t, "streamName", string(arg))
		}
	})

	test.RunWithMockDB(t, "Prepare data error", func(t *testing.T, db *sql.DB, _ sqlmock.Sqlmock) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("prepare data expected error")

		messages := []goengine.Message{
			mocks.NewDummyMessage(
				goengine.GenerateUUID(),
				[]byte(`{"Name":"alice","Balance":0}`),
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		persistenceStrategy := mockSQL.NewPersistenceStrategy(ctrl)
		persistenceStrategy.EXPECT().PrepareData(messages).Return(nil, expectedError).AnyTimes()
		persistenceStrategy.EXPECT().GenerateTableName(goengine.StreamName("orders")).Return("events_orders", nil).AnyTimes()
		persistenceStrategy.EXPECT().ColumnNames().Return([]string{"event_id", "event_name"}).AnyTimes()
		persistenceStrategy.EXPECT().EventColumnNames().Return([]string{"event_id", "event_name"}).AnyTimes()

		store, err := postgres.NewEventStore(persistenceStrategy, db, &mockSQL.MessageFactory{}, nil)
		require.NoError(t, err)

		err = store.AppendTo(context.Background(), "orders", messages)
		assert.Equal(t, expectedError, err)
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
			//{
			//	"With matcher",
			//	1,
			//	nil,
			//	func() metadata.Matcher {
			//		m := metadata.NewMatcher()
			//		m = metadata.WithConstraint(m, "version", metadata.GreaterThan, 1)
			//		m = metadata.WithConstraint(m, "version", metadata.LowerThan, 100)
			//		return m
			//	},
			//	`SELECT "no", "payload", "metadata" FROM event_stream WHERE no >= \$1 AND metadata ->> 'version' > \$2 AND metadata ->> 'version' < \$3 ORDER BY no`,
			//},
			{
				"Without matcher",
				1,
				nil,
				func() metadata.Matcher {
					return nil
				},
				`SELECT "no", "payload", "metadata" FROM event_stream WHERE no >= \$1 ORDER BY no`,
			},
			{
				"With limit",
				1,
				&limit50,
				func() metadata.Matcher {
					return nil
				},
				`SELECT "no", "payload", "metadata" FROM event_stream WHERE no >= \$1 ORDER BY no LIMIT 50`,
			},
		}

		for _, testCase := range testCases {
			test.RunWithMockDB(t, testCase.title, func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				matcher := testCase.matcher()

				expectedStream := &mocks.EventStream{}

				dbMock.ExpectQuery(testCase.expectedQuery).WillReturnRows(sqlmock.NewRows(columns))

				factory := mockSQL.NewMessageFactory(ctrl)
				factory.EXPECT().CreateEventStream(gomock.AssignableToTypeOf(&sql.Rows{})).Return(expectedStream, nil).Times(1)

				strategy := mockSQL.NewPersistenceStrategy(ctrl)
				strategy.EXPECT().PrepareSearch(matcher).Return([]byte{}, []interface{}{}).AnyTimes()
				strategy.EXPECT().ColumnNames().Return(columns).AnyTimes()
				strategy.EXPECT().EventColumnNames().Return(columns).AnyTimes()
				strategy.EXPECT().GenerateTableName(goengine.StreamName("event_stream")).Return("event_stream", nil).AnyTimes()

				store, err := postgres.NewEventStore(strategy, db, factory, nil)
				require.NoError(t, err)

				stream, err := store.Load(
					context.Background(),
					"event_stream",
					testCase.fromNumber,
					testCase.count,
					matcher,
				)

				assert.NoError(t, err)
				assert.Equal(t, expectedStream, stream)
			})
		}
	})

	t.Run("persistent strategy failures", func(t *testing.T) {
		columns := []string{"no", "payload"}

		testCases := []struct {
			title         string
			strategy      func(ctrl *gomock.Controller) *mockSQL.PersistenceStrategy
			expectedError error
		}{
			{
				"Empty table name returned",
				func(ctrl *gomock.Controller) *mockSQL.PersistenceStrategy {
					strategy := mockSQL.NewPersistenceStrategy(ctrl)
					strategy.EXPECT().ColumnNames().Return(columns).AnyTimes()
					strategy.EXPECT().EventColumnNames().Return(columns).AnyTimes()
					strategy.EXPECT().GenerateTableName(goengine.StreamName("event_stream")).
						Return("", nil).AnyTimes()
					return strategy
				},
				postgres.ErrTableNameEmpty,
			},
			{
				"Empty table name returned",
				func(ctrl *gomock.Controller) *mockSQL.PersistenceStrategy {
					strategy := mockSQL.NewPersistenceStrategy(ctrl)
					strategy.EXPECT().ColumnNames().Return(columns).AnyTimes()
					strategy.EXPECT().EventColumnNames().Return(columns).AnyTimes()
					strategy.EXPECT().GenerateTableName(goengine.StreamName("event_stream")).
						Return("", errors.New("failed gen")).AnyTimes()
					return strategy
				},
				errors.New("failed gen"),
			},
		}

		for _, testCase := range testCases {
			test.RunWithMockDB(t, testCase.title, func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				strategy := testCase.strategy(ctrl)
				store, err := postgres.NewEventStore(strategy, db, &mockSQL.MessageFactory{}, nil)
				require.NoError(t, err)

				stream, err := store.Load(context.Background(), "event_stream", 1, nil, nil)

				assert.Equal(t, testCase.expectedError, err)
				assert.Nil(t, stream)
			})
		}
	})
}

func mockHasStreamQuery(result bool, mock sqlmock.Sqlmock) {
	mockRows := sqlmock.NewRows([]string{"type"}).AddRow(result)
	mock.ExpectQuery(`SELECT EXISTS\((.+)`).WithArgs("events_orders").WillReturnRows(mockRows)
}

func mockMessages(ctrl *gomock.Controller) (*mocks.MessagePayloadConverter, []goengine.Message) {
	pc := mocks.NewMessagePayloadConverter(ctrl)
	messages := make([]goengine.Message, 3)

	for i := 0; i < len(messages); i++ {
		payload := []byte(fmt.Sprintf(`{"Name":"alice_%d","Balance":0}`, i))
		messages[i] = mocks.NewDummyMessage(
			goengine.GenerateUUID(),
			payload,
			metadata.FromMap(map[string]interface{}{
				"type":    fmt.Sprintf("m%d", i),
				"version": i + 1,
			}),
			time.Now(),
		)

		pc.EXPECT().ConvertPayload(payload).Return(fmt.Sprintf("Payload%d", i), payload, nil).AnyTimes()
	}

	return pc, messages
}

func createEventStore(t *testing.T, db *sql.DB, converter goengine.MessagePayloadConverter) *postgres.EventStore {
	persistenceStrategy, err := strategyPostgres.NewSingleStreamStrategy(converter)
	require.NoError(t, err)

	store, err := postgres.NewEventStore(persistenceStrategy, db, &mockSQL.MessageFactory{}, nil)
	require.NoError(t, err)

	return store
}

func BenchmarkEventStore_AppendToWithExecer(b *testing.B) {
	ctx := context.Background()
	ctrl := gomock.NewController(b)
	defer func() {
		b.StopTimer()
		ctrl.Finish()
	}()

	db, _, err := sqlmock.New()
	require.NoError(b, err)

	dbExecer := mockSQL.NewExecer(ctrl)
	dbExecer.EXPECT().ExecContext(ctx, gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	persistenceStrategy := mockSQL.NewPersistenceStrategy(ctrl)
	persistenceStrategy.EXPECT().ColumnNames().Return([]string{"event_id", "event_name", "payload", "metadata", "created_at"}).AnyTimes()
	persistenceStrategy.EXPECT().GenerateTableName(goengine.StreamName("hello")).Return("hello", nil).AnyTimes()
	persistenceStrategy.EXPECT().PrepareData(gomock.Any()).Return([]interface{}{
		"event_id", "event_name", "payload", "metadata", "created_at",
		"event_id", "event_name", "payload", "metadata", "created_at",
		"event_id", "event_name", "payload", "metadata", "created_at",
		"event_id", "event_name", "payload", "metadata", "created_at",
		"event_id", "event_name", "payload", "metadata", "created_at",
	}, nil).AnyTimes()
	require.NoError(b, err)

	messageFactory := mockSQL.NewMessageFactory(ctrl)
	messageFactory.EXPECT().CreateEventStream(gomock.Any()).Return(nil, nil).AnyTimes()

	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := store.AppendToWithExecer(ctx, dbExecer, "hello", make([]goengine.Message, 5))
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkEventStore_LoadWithConnection(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer func() {
		b.StopTimer()
		ctrl.Finish()
	}()

	ctx := context.Background()

	db, _, err := sqlmock.New()
	require.NoError(b, err)

	dbQueryer := mockSQL.NewQueryer(ctrl)
	dbQueryer.EXPECT().QueryContext(ctx, gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	persistenceStrategy := mockSQL.NewPersistenceStrategy(ctrl)
	persistenceStrategy.EXPECT().ColumnNames().Return([]string{"event_id", "event_name", "payload", "metadata", "created_at"}).AnyTimes()
	persistenceStrategy.EXPECT().GenerateTableName(goengine.StreamName("hello")).Return("hello", nil).AnyTimes()
	require.NoError(b, err)

	messageFactory := mockSQL.NewMessageFactory(ctrl)
	messageFactory.EXPECT().CreateEventStream(gomock.Any()).Return(nil, nil).AnyTimes()

	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	require.NoError(b, err)

	matcher := metadata.NewMatcher()
	matcher = metadata.WithConstraint(matcher, aggregate.TypeKey, metadata.Equals, "test")
	matcher = metadata.WithConstraint(matcher, aggregate.IDKey, metadata.Equals, "lalalalala")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := store.LoadWithConnection(
			ctx,
			dbQueryer,
			"hello",
			10,
			nil,
			matcher,
		)
		if err != nil {
			b.Error(res, err)
		}
	}
}
