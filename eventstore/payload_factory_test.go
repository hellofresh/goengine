package eventstore_test

import (
	"testing"

	"encoding/json"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/stretchr/testify/assert"
)

type simpleType struct {
	Test  string
	Order int
}

func TestJSONPayloadFactory_CreatePayload(t *testing.T) {
	t.Run("payload creation", func(t *testing.T) {
		type validTestCase struct {
			title            string
			payloadType      string
			payloadInitiator eventstore.PayloadInitiator
			payloadData      interface{}
			expectedData     interface{}
		}

		testCases := []validTestCase{
			{
				"[]byte string slice",
				"string_slice",
				func() interface{} {
					return []string{}
				},
				[]byte(`["test","123","lala"]`),
				[]string{"test", "123", "lala"},
			},
			{
				"struct",
				"struct",
				func() interface{} {
					return simpleType{}
				},
				json.RawMessage(`{"test":"mine","order":1}`),
				simpleType{Test: "mine", Order: 1},
			},
			{
				"struct",
				"prt_struct",
				func() interface{} {
					return &simpleType{}
				},
				`{"test":"mine","order":1}`,
				&simpleType{Test: "mine", Order: 1},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)

				factory := eventstore.NewJSONPayloadFactory()
				err := factory.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)
				if !asserts.Nil(err) {
					return
				}

				payload, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				asserts.EqualValues(testCase.expectedData, payload)
				asserts.Nil(err)
			})
		}
	})

	t.Run("invalid arguments", func(t *testing.T) {
		type invalidTestCase struct {
			title         string
			payloadType   string
			payloadData   interface{}
			expectedError error
		}

		testCases := []invalidTestCase{
			{
				"struct payload data",
				"test",
				struct{}{},
				eventstore.ErrUnsupportedJSONPayloadData,
			},
			{
				"unknown payload type",
				"test",
				[]byte{},
				eventstore.ErrUnknownPayloadType,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := eventstore.NewJSONPayloadFactory()
				payload, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err)
				asserts.Nil(payload)
			})
		}
	})
}

func TestJSONPayloadFactory_RegisterPayload(t *testing.T) {
	t.Run("register a type", func(t *testing.T) {
		factory := eventstore.NewJSONPayloadFactory()
		err := factory.RegisterPayload("test", func() interface{} {
			return &struct{ order int }{}
		})

		assert.Nil(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := factory.RegisterPayload("test", func() interface{} {
				return &struct{ order int }{}
			})

			assert.Equal(t, eventstore.ErrDuplicatePayloadType, err)
		})
	})

	t.Run("failed registrations", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadType      string
			payloadInitiator eventstore.PayloadInitiator
			expectedError    error
		}

		testCases := []invalidTestCase{
			{
				"nil initiator",
				"nil",
				func() interface{} {
					return nil
				},
				eventstore.ErrInitiatorInvalidResult,
			},
			{
				"nil reference initiator",
				"nil",
				func() interface{} {
					return (*invalidTestCase)(nil)
				},
				eventstore.ErrInitiatorInvalidResult,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := eventstore.NewJSONPayloadFactory()
				err := factory.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)

				assert.Equal(t, testCase.expectedError, err)
			})
		}
	})
}
