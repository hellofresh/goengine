// +build unit

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/hellofresh/goengine/internal/test"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestEventStore_Create(t *testing.T) {
	test.RunWithMockDB(t, "Check create table with indexes", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		mockHasStreamQuery(false, dbMock)
		dbMock.ExpectBegin()
		dbMock.ExpectExec(`CREATE TABLE "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectExec(`CREATE UNIQUE INDEX "events_orders_unique_index___aggregate_type__aggregate_id__aggregate_version" ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		dbMock.ExpectExec(`CREATE INDEX "events_orders_index__aggregate_type__aggregate_id" ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
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
}

func TestEventStore_HasStream(t *testing.T) {
	testCases := []struct {
		title      string
		streamName eventstore.StreamName
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
		messages := []messaging.Message{
			mockMessage(
				messaging.GenerateUUID(),
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

		messages := []messaging.Message{
			mockMessage(
				messaging.GenerateUUID(),
				[]byte(`{"Name":"alice","Balance":0}`),
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		persistenceStrategy := &mocks.PersistenceStrategy{}
		persistenceStrategy.On("PrepareData", messages).Return(nil, expectedError)
		persistenceStrategy.On("GenerateTableName", eventstore.StreamName("orders")).Return("events_orders", nil)
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

func mockHasStreamQuery(result bool, mock sqlmock.Sqlmock) {
	mockRows := sqlmock.NewRows([]string{"type"}).AddRow(result)
	mock.ExpectQuery(`SELECT EXISTS\((.+)`).WithArgs("events_orders").WillReturnRows(mockRows)
}

func mockMessages() (*mocks.PayloadConverter, []messaging.Message) {
	pc := &mocks.PayloadConverter{}
	messages := make([]messaging.Message, 3)

	for i := 0; i < len(messages); i++ {
		payload := []byte(fmt.Sprintf(`{"Name":"alice_%d","Balance":0}`, i))
		messages[i] = mockMessage(
			messaging.GenerateUUID(),
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

func createEventStore(t *testing.T, db *sql.DB, converter eventstore.PayloadConverter) eventstore.EventStore {
	asserts := assert.New(t)

	persistenceStrategy, err := postgres.NewPostgresStrategy(converter)
	if !asserts.NoError(err) {
		t.Fail()
	}

	messageFactory := &mocks.MessageFactory{}
	if !asserts.NoError(err) {
		t.Fail()
	}

	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	if !asserts.NoError(err) {
		t.Fail()
	}

	return store
}
