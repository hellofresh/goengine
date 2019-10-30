// +build unit

package postgres_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/hellofresh/goengine/strategy/json/internal"
	"github.com/hellofresh/goengine/strategy/json/sql/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPostgresStrategy(t *testing.T) {
	t.Run("error on no converter provided", func(t *testing.T) {
		strategy, err := postgres.NewSingleStreamStrategy(nil)

		if assert.Error(t, err) {
			arg := err.(goengine.InvalidArgumentError)
			assert.Equal(t, "converter", string(arg))
		}
		assert.Nil(t, strategy)
	})

	t.Run("error on no converter provided", func(t *testing.T) {
		strategy, err := postgres.NewSingleStreamStrategy(&mocks.MessagePayloadConverter{})

		assert.IsTypef(t, &postgres.SingleStreamStrategy{}, strategy, "")
		assert.Nil(t, err)
	})
}

func TestGenerateTableName(t *testing.T) {
	strategy, err := postgres.NewSingleStreamStrategy(&mocks.MessagePayloadConverter{})
	require.NoError(t, err)

	t.Run("name conversions", func(t *testing.T) {
		type validTestCase struct {
			title  string
			input  goengine.StreamName
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

				assert.NoError(t, err)
				assert.Equal(t, testCase.output, tableName)
			})
		}
	})

	t.Run("Empty stream names are not supported", func(t *testing.T) {
		tableName, err := strategy.GenerateTableName("")

		asserts := assert.New(t)
		asserts.Empty(tableName)
		if asserts.Error(err) {
			arg := err.(goengine.InvalidArgumentError)
			asserts.Equal("streamName", string(arg))
		}
	})
}

func TestColumnNames(t *testing.T) {
	expectedColumns := []string{"event_id", "event_name", "payload", "metadata", "aggregate_type", "aggregate_id", "aggregate_version", "created_at"}

	strategy, err := postgres.NewSingleStreamStrategy(&mocks.MessagePayloadConverter{})
	require.NoError(t, err)

	t.Run("get expected columns", func(t *testing.T) {
		cols := strategy.InsertColumnNames()

		assert.Equal(t, cols, expectedColumns)
	})

	t.Run("cannot modify data", func(t *testing.T) {
		assert.Equal(t, strategy.InsertColumnNames(), expectedColumns)
	})
}

func TestCreateSchema(t *testing.T) {
	strategy, err := postgres.NewSingleStreamStrategy(&mocks.MessagePayloadConverter{})
	require.NoError(t, err)

	t.Run("output statement elements count", func(t *testing.T) {
		cs := strategy.CreateSchema("abc")

		assert.Equal(t, 3, len(cs))
		assert.Contains(t, cs[0], `CREATE TABLE "abc"`)
	})
}

func TestPrepareData(t *testing.T) {
	t.Run("get expected columns", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pc := mocks.NewMessagePayloadConverter(ctrl)
		messages := make([]goengine.Message, 3)
		expectedColumns := make([]interface{}, 0, 3*8)
		for i := range messages {
			id := goengine.GenerateUUID()
			payload := []byte(fmt.Sprintf(`{"Name":"%d","Balance":0}`, i))
			payloadType := fmt.Sprintf("Payload%d", i)
			meta := metadata.FromMap(map[string]interface{}{"type": "m", "version": i})
			createdAt := time.Now()

			messages[i] = mocks.NewDummyMessage(id, payload, meta, createdAt)

			pc.EXPECT().ConvertPayload(payload).Return(payloadType, payload, nil).AnyTimes()

			metaJSON, err := internal.MarshalJSON(meta)
			require.NoError(t, err)

			expectedColumns = append(
				expectedColumns,
				id,
				payloadType,
				payload,
				metaJSON,
				nil,
				nil,
				nil,
				createdAt,
			)
		}

		strategy, err := postgres.NewSingleStreamStrategy(pc)
		require.NoError(t, err)

		data, err := strategy.PrepareData(messages)

		assert.NoError(t, err)
		assert.Equal(t, expectedColumns, data)
	})

	t.Run("Converter error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("converter error")

		payload := []byte(`{"Name":"alice","Balance":0}`)

		messages := []goengine.Message{
			mocks.NewDummyMessage(
				goengine.GenerateUUID(),
				payload,
				metadata.FromMap(map[string]interface{}{"type": "m1", "version": 1}),
				time.Now(),
			),
		}

		pc := mocks.NewMessagePayloadConverter(ctrl)
		pc.EXPECT().ConvertPayload(payload).Return("PayloadFirst", nil, expectedErr).Times(1)

		strategy, err := postgres.NewSingleStreamStrategy(pc)
		require.NoError(t, err)

		data, err := strategy.PrepareData(messages)

		assert.Error(t, expectedErr, err)
		assert.Nil(t, data)
	})
}
