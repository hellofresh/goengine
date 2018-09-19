package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/mocks"
)

func TestCreate(t *testing.T) {
	asserts := assert.New(t)
	t.Run("Check create table with indexes", func(t *testing.T) {
		db, mock, _ := sqlmock.New()

		mockHasStreamQuery(false, mock)
		mock.ExpectBegin()
		mock.ExpectExec(`CREATE TABLE "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE UNIQUE INDEX "events_orders_unique_index___aggregate_type__aggregate_id__aggregate_version" ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE INDEX "events_orders_index__aggregate_type__aggregate_id" ON "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		store := eventStore(asserts, db)
		err := store.Create(context.Background(), "orders")
		asserts.NoError(err)
	})

	t.Run("Check transaction rollback", func(t *testing.T) {
		ctx := context.Background()
		db, mock, err := sqlmock.New()
		if !assert.Nil(t, err) {
			return
		}
		defer db.Close()

		expectedError := errors.New("index error")
		mockHasStreamQuery(false, mock)
		mock.ExpectBegin()
		mock.ExpectExec(`CREATE TABLE "events_orders"(.+)`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`CREATE UNIQUE INDEX(.+)ON "events_orders"(.+)`).WillReturnError(expectedError)
		mock.ExpectRollback()

		store := eventStore(asserts, db)
		err = store.Create(ctx, "orders")
		asserts.Error(err)
		asserts.Equal(expectedError, err)
	})

	t.Run("Empty stream name", func(t *testing.T) {
		ctx := context.Background()
		db, _, err := sqlmock.New()
		if !assert.Nil(t, err) {
			return
		}
		defer db.Close()

		store := eventStore(asserts, db)
		err = store.Create(ctx, "")
		assert.Error(t, err)
		assert.Equal(t, postgres.ErrEmptyStreamName, err)
	})

	t.Run("Stream table already exist", func(t *testing.T) {
		ctx := context.Background()
		db, mock, err := sqlmock.New()
		if !assert.Nil(t, err) {
			return
		}
		defer db.Close()

		mockHasStreamQuery(true, mock)

		store := eventStore(asserts, db)
		err = store.Create(ctx, "orders")
		asserts.Error(err)
		asserts.Equal(postgres.ErrTableAlreadyExists, err)
	})
}

func TestHasStream(t *testing.T) {
	asserts := assert.New(t)
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
		t.Run(testCase.title, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			mockHasStreamQuery(testCase.sqlResult, mock)
			store := eventStore(asserts, db)
			b := store.HasStream(context.Background(), testCase.streamName)
			assert.Equal(t, testCase.expected, b)
		})
	}
}

func mockHasStreamQuery(result bool, mock sqlmock.Sqlmock) {
	mockRows := sqlmock.NewRows([]string{"type"})
	mockRows.AddRow(result)
	mock.ExpectQuery(`SELECT EXISTS\((.+)`).WithArgs("events_orders").WillReturnRows(mockRows)
}

func TestAppendTo(t *testing.T) {
	asserts := assert.New(t)
	t.Run("Insert successfully", func(t *testing.T) {
		payloadConverter, messages := mockMessages()
		db, mock, _ := sqlmock.New()
		mock.ExpectExec(`INSERT(.+)VALUES \(\$1,\$2,\$3,\$4,\$5\),\(\$6,\$7,\$8,\$9,\$10\),\(\$11(.+)`).
			WillReturnResult(sqlmock.NewResult(111, 3))

		persistenceStrategy, err := postgres.NewPostgresStrategy(payloadConverter)
		messageFactory := &mocks.MessageFactory{}
		asserts.NoError(err)

		eventStore, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
		asserts.NoError(err)

		err = eventStore.AppendTo(context.Background(), "orders", messages)
		asserts.NoError(err)
	})

	t.Run("Empty stream name", func(t *testing.T) {
		id := messaging.GenerateUUID()
		meta := getMeta(map[string]interface{}{"type": "m1", "version": 1})
		payload := []byte(`{"Name":"alice","Balance":0}`)
		message := mockMessage(id, payload, meta, time.Now())
		payloadConverter := &mocks.PayloadConverter{}

		messages := []messaging.Message{message}
		ctx := context.Background()
		db, _, _ := sqlmock.New()

		persistenceStrategy, err := postgres.NewPostgresStrategy(payloadConverter)
		mf := &mocks.MessageFactory{}
		asserts.NoError(err)

		store, err := postgres.NewEventStore(persistenceStrategy, db, mf, nil)
		asserts.NoError(err)

		err = store.AppendTo(ctx, "", messages)
		asserts.Error(err)
		asserts.Equal(postgres.ErrEmptyStreamName, err)
	})

	t.Run("Prepare data error", func(t *testing.T) {
		expectedError := errors.New("prepare data expected error")
		id := messaging.GenerateUUID()
		meta := getMeta(map[string]interface{}{"type": "m1", "version": 1})
		payload := []byte(`{"Name":"alice","Balance":0}`)
		message := mockMessage(id, payload, meta, time.Now())
		messages := []messaging.Message{message}

		db, _, _ := sqlmock.New()
		ctx := context.Background()
		persistenceStrategy := &mocks.PersistenceStrategy{}
		persistenceStrategy.On("PrepareData", messages).Return(nil, expectedError)
		streamName := eventstore.StreamName("orders")
		persistenceStrategy.On("GenerateTableName", streamName).Return("events_orders", nil)
		persistenceStrategy.On("ColumnNames").Return([]string{"event_id", "event_name"})
		messageFactory := &mocks.MessageFactory{}

		store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
		asserts.NoError(err)

		err = store.AppendTo(ctx, "orders", messages)
		asserts.Error(err)
		asserts.Equal(expectedError, err)
	})
}

func mockMessages() (*mocks.PayloadConverter, []messaging.Message) {
	id1 := messaging.GenerateUUID()
	id2 := messaging.GenerateUUID()
	id3 := messaging.GenerateUUID()
	meta1 := getMeta(map[string]interface{}{"type": "m1", "version": 1})
	meta2 := getMeta(map[string]interface{}{"type": "m1", "version": 2})
	meta3 := getMeta(map[string]interface{}{"type": "m1", "version": 3})
	payload1 := []byte(`{"Name":"alice","Balance":0}`)
	payload2 := []byte(`{"Add":1}`)
	payload3 := []byte(`{"Add":2}`)
	m1 := mockMessage(id1, payload1, meta1, time.Now())
	m2 := mockMessage(id2, payload2, meta2, time.Now())
	m3 := mockMessage(id3, payload3, meta3, time.Now())
	pc := &mocks.PayloadConverter{}
	pc.On("ConvertPayload", payload1).Return("PayloadFirst", payload1, nil)
	pc.On("ConvertPayload", payload2).Return("PayloadSecond", payload2, nil)
	pc.On("ConvertPayload", payload3).Return("PayloadThird", payload3, nil)
	messages := []messaging.Message{m1, m2, m3}
	return pc, messages
}

func eventStore(asserts *assert.Assertions, db *sql.DB) eventstore.EventStore {
	payloadConverter := &mocks.PayloadConverter{}
	persistenceStrategy, err := postgres.NewPostgresStrategy(payloadConverter)
	asserts.NoError(err)
	messageFactory := &mocks.MessageFactory{}
	asserts.NoError(err)
	store, err := postgres.NewEventStore(persistenceStrategy, db, messageFactory, nil)
	asserts.NoError(err)

	return store
}
