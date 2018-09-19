package postgres_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewPostgresStrategy(t *testing.T) {
	t.Run("error on no converter provided", func(t *testing.T) {
		strategy, err := postgres.NewPostgresStrategy(nil)

		assert.Error(t, postgres.ErrNoPayloadConverter, err)
		assert.Nil(t, strategy)
	})

	t.Run("error on no converter provided", func(t *testing.T) {
		strategy, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})

		assert.IsTypef(t, &postgres.SingleStreamStrategy{}, strategy, "")
		assert.Nil(t, err)
	})
}

func TestGenerateTableName(t *testing.T) {
	strategy, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
	if err != nil {
		t.Fatal("Strategy could not be initiated", err)
	}

	t.Run("name conversions", func(t *testing.T) {
		type validTestCase struct {
			title  string
			input  eventstore.StreamName
			output string
		}

		testCases := []validTestCase{
			{
				"no escaping: letters",
				"order",
				"events_order",
			},
			{
				"no escaping: letters, numbers",
				"order1",
				"events_order1",
			},
			{
				"no escaping: letters, numbers",
				"order1",
				"events_order1",
			},
			{
				"no escaping: underscores",
				"order_1_",
				"events_order_1",
			},
			{
				"escaping: brackets []",
				"order[1]",
				"events_order1",
			},
			{
				"escaping: brackets ()",
				"order(1)",
				"events_order1",
			},
			{
				"escaping: special symbols",
				"order%1#?",
				"events_order1",
			},
			{
				"escaping: special symbols",
				"o.r,d;e:r%1#?",
				"events_order1",
			},
			{
				"escaping: quotes",
				"order'1\"",
				"events_order1",
			},
			{
				"escaping: dash, slash",
				"order\\-1-",
				"events_order1",
			},
			{
				"escaping: dash, slash",
				"or_de_r___",
				"events_or_de_r",
			},
			{
				"escaping: dash, slash",
				"or_de_r__&@#_",
				"events_or_de_r",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				tableName, err := strategy.GenerateTableName(testCase.input)

				asserts := assert.New(t)
				if asserts.NoError(err) {
					asserts.Equal(testCase.output, tableName)
				}
			})
		}
	})

	t.Run("Empty stream names are not supported", func(t *testing.T) {
		tableName, err := strategy.GenerateTableName("")

		asserts := assert.New(t)
		asserts.Empty(tableName)
		asserts.Equal(postgres.ErrEmptyStreamName, err)
	})
}

func TestColumnNames(t *testing.T) {
	expectedColumns := []string{"event_id", "event_name", "payload", "metadata", "created_at"}

	strategy, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
	if err != nil {
		t.Fatal("Strategy could not be initiated", err)
	}

	t.Run("get expected columns", func(t *testing.T) {
		cols := strategy.ColumnNames()

		assert.Equal(t, cols, expectedColumns)
	})

	t.Run("cannot modify data", func(t *testing.T) {
		colsOrig := strategy.ColumnNames()
		colsOrig = append(colsOrig, "field1")
		colsOrig = append(colsOrig, "field2")

		assert.Equal(t, strategy.ColumnNames(), expectedColumns)
	})
}

func TestCreateSchema(t *testing.T) {
	strategy, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
	if err != nil {
		t.Fatal("Strategy could not be initiated", err)
	}

	t.Run("output statement elements count", func(t *testing.T) {
		cs := strategy.CreateSchema("abc")

		assert.Equal(t, 3, len(cs))
		assert.Contains(t, cs[0], `CREATE TABLE "abc"`)
	})
}

func TestPrepareData(t *testing.T) {
	t.Run("get expected columns", func(t *testing.T) {
		asserts := assert.New(t)

		pc := &mocks.PayloadConverter{}
		messages := make([]messaging.Message, 3)
		expectedColumns := make([]interface{}, 3*5)
		for i := range messages {
			id := messaging.GenerateUUID()
			payload := []byte(fmt.Sprintf(`{"Name":"%d","Balance":0}`, i))
			payloadType := fmt.Sprintf("Payload%d", i)
			meta := metadata.FromMap(map[string]interface{}{"type": "m", "version": i})
			createdAt := time.Now()

			messages[i] = mockMessage(id, payload, meta, createdAt)

			pc.On("ConvertPayload", payload).Return(payloadType, payload, nil)

			metaJSON, err := json.Marshal(meta)
			if !asserts.NoError(err) {
				return
			}

			expectedColumns = append(
				expectedColumns,
				id,
				payloadType,
				payload,
				metaJSON,
				createdAt,
			)
		}

		strategy, err := postgres.NewPostgresStrategy(pc)
		if asserts.NoError(err) {
			return
		}

		data, err := strategy.PrepareData(messages)

		asserts.Equal(expectedColumns, data)
		asserts.Equal(nil, err)
	})

	t.Run("Converter error", func(t *testing.T) {
		asserts := assert.New(t)

		expectedErr := errors.New("converter error")

		payload := []byte(`{"Name":"alice","Balance":0}`)

		messages := []messaging.Message{
			mockMessage(
				messaging.GenerateUUID(),
				payload,
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		pc := &mocks.PayloadConverter{}
		pc.On("ConvertPayload", payload).Return("PayloadFirst", nil, expectedErr)

		strategy, err := postgres.NewPostgresStrategy(pc)
		if asserts.NoError(err) {
			return
		}

		data, err := strategy.PrepareData(messages)

		asserts.Error(expectedErr, err)
		asserts.Nil(data)
	})
}

func mockMessage(id messaging.UUID, payload []byte, meta interface{}, time time.Time) *mocks.Message {
	m := &mocks.Message{}
	m.On("UUID").Return(id)
	m.On("Payload").Return(payload)
	m.On("Metadata").Return(meta)
	m.On("CreatedAt").Return(time)
	return m
}
