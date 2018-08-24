package postgres_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/postgres"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
)

func TestNewPostgresStrategy(t *testing.T) {
	t.Run("error on no converter provided", func(t *testing.T) {
		s, err := postgres.NewPostgresStrategy(nil)
		assert.Error(t, postgres.ErrorNoPayloadConverter, err)
		assert.Nil(t, s)
	})

	t.Run("error on no converter provided", func(t *testing.T) {
		s, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
		assert.IsTypef(t, &postgres.SingleStreamStrategy{}, s, "")
		assert.Nil(t, err)
	})
}

func TestGenerateTableName(t *testing.T) {
	type testCase struct {
		title    string
		input    eventstore.StreamName
		expected string
		err      error
	}

	testCases := []testCase{
		{
			"Empty stream name",
			"",
			"",
			postgres.ErrorEmptyStreamName,
		},
		{
			"no escaping: letters",
			"order",
			"events_order",
			nil,
		},
		{
			"no escaping: letters, numbers",
			"order1",
			"events_order1",
			nil,
		},
		{
			"no escaping: letters, numbers",
			"order1",
			"events_order1",
			nil,
		},
		{
			"no escaping: underscores",
			"order_1_",
			"events_order_1",
			nil,
		},
		{
			"escaping: brackets []",
			"order[1]",
			"events_order1",
			nil,
		},
		{
			"escaping: brackets ()",
			"order(1)",
			"events_order1",
			nil,
		},
		{
			"escaping: special symbols",
			"order%1#?",
			"events_order1",
			nil,
		},
		{
			"escaping: special symbols",
			"o.r,d;e:r%1#?",
			"events_order1",
			nil,
		},
		{
			"escaping: quotes",
			"order'1\"",
			"events_order1",
			nil,
		},
		{
			"escaping: dash, slash",
			"order\\-1-",
			"events_order1",
			nil,
		},
		{
			"escaping: dash, slash",
			"or_de_r___",
			"events_or_de_r",
			nil,
		},
		{
			"escaping: dash, slash",
			"or_de_r__&@#_",
			"events_or_de_r",
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			s, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
			assert.Nil(t, err)
			tableName, err := s.GenerateTableName(testCase.input)
			assert.Equal(t, testCase.err, err)
			assert.Equal(t, testCase.expected, tableName)
		})
	}
}

func TestColumnNames(t *testing.T) {
	expected := []string{"event_id", "event_name", "payload", "metadata", "created_at"}
	s, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
	t.Run("get expected columns", func(t *testing.T) {
		assert.Nil(t, err)
		cols := s.ColumnNames()
		assert.Equal(t, cols, expected)
	})

	t.Run("cannot modify data", func(t *testing.T) {
		colsOrig := s.ColumnNames()
		colsOrig = append(colsOrig, "field1")
		colsOrig = append(colsOrig, "field2")
		assert.Equal(t, s.ColumnNames(), expected)
	})
}

func TestCreateSchema(t *testing.T) {
	t.Run("output statement elements count", func(t *testing.T) {
		s, err := postgres.NewPostgresStrategy(&mocks.PayloadConverter{})
		assert.Nil(t, err)
		cs := s.CreateSchema("abc")
		assert.Equal(t, 3, len(cs))
		assert.Contains(t, cs[0], `CREATE TABLE "abc"`)
	})
}

func TestPrepareData(t *testing.T) {
	t.Run("get expected columns", func(t *testing.T) {
		id1 := messaging.GenerateUUID()
		id2 := messaging.GenerateUUID()
		id3 := messaging.GenerateUUID()

		meta1 := getMeta(map[string]interface{}{"type": "m1", "version": 1})
		metab1, _ := json.Marshal(meta1)
		meta2 := getMeta(map[string]interface{}{"type": "m1", "version": 2})
		metab2, _ := json.Marshal(meta2)
		meta3 := getMeta(map[string]interface{}{"type": "m1", "version": 3})
		metab3, _ := json.Marshal(meta3)

		payload1 := []byte(`{"Name":"alice","Balance":0}`)
		payload2 := []byte(`{"Add":1}`)
		payload3 := []byte(`{"Add":2}`)

		m1 := getMessage(id1, payload1, meta1, time.Now())
		m2 := getMessage(id2, payload2, meta2, time.Now())
		m3 := getMessage(id3, payload3, meta3, time.Now())

		pc := &mocks.PayloadConverter{}
		pc.On("ConvertPayload", payload1).Return("PayloadFirst", payload1, nil)
		pc.On("ConvertPayload", payload2).Return("PayloadSecond", payload2, nil)
		pc.On("ConvertPayload", payload3).Return("PayloadThird", payload3, nil)

		messages := []messaging.Message{m1, m2, m3}
		s, err := postgres.NewPostgresStrategy(pc)
		assert.Nil(t, err)
		data, err := s.PrepareData(messages)

		assert.Equal(t, 15, len(data))
		assert.Equal(t, nil, err)

		// check UUID
		assert.Equal(t, id1, data[0])
		assert.Equal(t, id2, data[5])
		assert.Equal(t, id3, data[10])

		// check payload type
		assert.Equal(t, "PayloadFirst", data[1])
		assert.Equal(t, "PayloadSecond", data[6])
		assert.Equal(t, "PayloadThird", data[11])

		// check payload
		assert.Equal(t, payload1, data[2])
		assert.Equal(t, payload2, data[7])
		assert.Equal(t, payload3, data[12])

		// check metadata
		assert.Equal(t, metab1, data[3])
		assert.Equal(t, metab2, data[8])
		assert.Equal(t, metab3, data[13])

		// check dates
		assert.IsTypef(t, time.Time{}, data[4], "type of time")
		assert.IsTypef(t, time.Time{}, data[9], "type of time")
		assert.IsTypef(t, time.Time{}, data[14], "type of time")
	})

	t.Run("Converter error", func(t *testing.T) {
		id := messaging.GenerateUUID()
		meta := getMeta(map[string]interface{}{"type": "m1", "version": 1})
		payload := []byte(`{"Name":"alice","Balance":0}`)

		m := getMessage(id, payload, meta, time.Now())
		pc := &mocks.PayloadConverter{}
		expectedErr := errors.New("Converter error")
		pc.On("ConvertPayload", payload).Return("PayloadFirst", nil, expectedErr)

		messages := []messaging.Message{m}
		s, err := postgres.NewPostgresStrategy(pc)
		assert.Nil(t, err)
		data, err := s.PrepareData(messages)
		assert.Error(t, expectedErr, err)
		assert.Nil(t, data)
	})
}

func getMessage(id messaging.UUID, payload []byte, meta interface{}, time time.Time) *mocks.Message {
	m := &mocks.Message{}
	m.On("UUID").Return(id)
	m.On("Payload").Return(payload)
	m.On("Metadata").Return(meta)
	m.On("CreatedAt").Return(time)
	return m
}

func getMeta(metadataInfo map[string]interface{}) metadata.Metadata {
	meta := metadata.New()
	for key, val := range metadataInfo {
		meta = metadata.WithValue(meta, key, val)
	}
	return meta
}
