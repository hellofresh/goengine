package json_test

import (
	"encoding/json"
	"testing"

	eventstorejson "github.com/hellofresh/goengine/eventstore/json"
	"github.com/stretchr/testify/assert"
)

type simpleType struct {
	Test  string
	Order int
}

func TestPayloadTransformer_ConvertPayload(t *testing.T) {

	t.Run("valid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadType      string
			payloadInitiator eventstorejson.PayloadInitiator
			payloadData      interface{}
			expectedName     string
			expectedData     interface{}
		}

		testCases := []testCase{
			{
				"convert payload",
				"tests",
				func() interface{} {
					return &simpleType{}
				},
				&simpleType{Test: "test", Order: 1},
				"tests",
				[]byte(`{"Test":"test","Order":1}`),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := eventstorejson.NewPayloadTransformer()
				transformer.RegisterPayload(tc.payloadType, tc.payloadInitiator)

				name, data, err := transformer.ConvertPayload(tc.payloadData)
				asserts.NoError(err)
				asserts.Equal(tc.expectedName, name)
				asserts.Equal(tc.expectedData, data)
			})
		}
	})

	t.Run("invalid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadType      string
			registerPayload  bool
			payloadInitiator eventstorejson.PayloadInitiator
			payloadData      interface{}
			expectedError    error
			expectedName     string
			expectedData     interface{}
		}

		testCases := []testCase{
			{
				"not registered convert payload",
				"",
				false,
				func() interface{} {
					// not necessary for this test case
					return nil
				},
				&simpleType{Test: "test", Order: 1},
				eventstorejson.ErrPayloadNotRegistered,
				"",
				[]byte(nil),
			},
			{
				"error marshalling payload",
				"tests",
				true,
				func() interface{} {
					// Need to register something that is not json serializable.
					return func() {}
				},
				func() {},
				eventstorejson.ErrPayloadCannotBeSerialized,
				"",
				[]byte(nil),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := eventstorejson.NewPayloadTransformer()

				if tc.registerPayload {
					transformer.RegisterPayload(tc.payloadType, tc.payloadInitiator)
				}

				name, data, err := transformer.ConvertPayload(tc.payloadData)
				asserts.Equal(tc.expectedError, err)
				asserts.Equal(tc.expectedName, name)
				asserts.Equal(tc.expectedData, data)
			})
		}
	})
}

func TestJSONPayloadTransformer_CreatePayload(t *testing.T) {
	t.Run("payload creation", func(t *testing.T) {
		type validTestCase struct {
			title            string
			payloadType      string
			payloadInitiator eventstorejson.PayloadInitiator
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

				factory := eventstorejson.NewPayloadTransformer()
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
				eventstorejson.ErrUnsupportedJSONPayloadData,
			},
			{
				"unknown payload type",
				"test",
				[]byte{},
				eventstorejson.ErrUnknownPayloadType,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := eventstorejson.NewPayloadTransformer()
				payload, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err)
				asserts.Nil(payload)
			})
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadInitiator eventstorejson.PayloadInitiator
			payloadData      interface{}
		}

		testCases := []invalidTestCase{
			{
				"bad json",
				func() interface{} {
					return simpleType{}
				},
				`{ bad: json }`,
			},
			{
				"bad json for a reference type",
				func() interface{} {
					return &simpleType{}
				},
				[]byte(`["comma to much",]`),
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := eventstorejson.NewPayloadTransformer()
				factory.RegisterPayload("tests", testCase.payloadInitiator)

				payload, err := factory.CreatePayload("tests", testCase.payloadData)

				asserts := assert.New(t)
				asserts.IsType((*json.SyntaxError)(nil), err)
				asserts.Nil(payload)
			})
		}
	})
}

func TestJSONPayloadTransformer_RegisterPayload(t *testing.T) {
	t.Run("register a type", func(t *testing.T) {
		transformer := eventstorejson.NewPayloadTransformer()
		err := transformer.RegisterPayload("test", func() interface{} {
			return &struct{ order int }{}
		})

		assert.Nil(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := transformer.RegisterPayload("test", func() interface{} {
				return &struct{ order int }{}
			})

			assert.Equal(t, eventstorejson.ErrDuplicatePayloadType, err)
		})
	})

	t.Run("failed registrations", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadType      string
			payloadInitiator eventstorejson.PayloadInitiator
			expectedError    error
		}

		testCases := []invalidTestCase{
			{
				"nil initiator",
				"nil",
				func() interface{} {
					return nil
				},
				eventstorejson.ErrInitiatorInvalidResult,
			},
			{
				"nil reference initiator",
				"nil",
				func() interface{} {
					return (*invalidTestCase)(nil)
				},
				eventstorejson.ErrInitiatorInvalidResult,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				transformer := eventstorejson.NewPayloadTransformer()
				err := transformer.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)

				assert.Equal(t, testCase.expectedError, err)
			})
		}
	})
}
