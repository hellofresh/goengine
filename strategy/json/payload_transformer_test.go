//go:build unit
// +build unit

package json_test

import (
	"encoding/json"
	"testing"

	anotherpayload "github.com/hellofresh/goengine/v2/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/v2/internal/mocks/payload"
	strategyJSON "github.com/hellofresh/goengine/v2/strategy/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type simpleType struct {
	Test  string
	Order int
}

func TestPayloadTransformer(t *testing.T) {
	t.Run("same type on different packages", func(t *testing.T) {
		asserts := assert.New(t)

		transformer := strategyJSON.NewPayloadTransformer()
		require.NoError(t,
			transformer.RegisterPayload("payload", func() interface{} {
				return payload.Payload{}
			}),
		)

		name, data, err := transformer.ConvertPayload(anotherpayload.Payload{})
		asserts.Equal(err, strategyJSON.ErrPayloadNotRegistered)
		asserts.Equal("", name)
		asserts.Equal([]byte(nil), data)
	})
}

func TestPayloadTransformer_ConvertPayload(t *testing.T) {

	t.Run("valid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadType      string
			payloadInitiator strategyJSON.PayloadInitiator
			payloadData      interface{}
			expectedData     string
		}

		testCases := []testCase{
			{
				"convert payload",
				"tests",
				func() interface{} {
					return &simpleType{}
				},
				&simpleType{Test: "test", Order: 1},
				`{"Test":"test","Order":1}`,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := strategyJSON.NewPayloadTransformer()
				require.NoError(t,
					transformer.RegisterPayload(tc.payloadType, tc.payloadInitiator),
				)

				name, data, err := transformer.ConvertPayload(tc.payloadData)
				asserts.NoError(err)
				asserts.Equal(tc.payloadType, name)
				asserts.JSONEq(tc.expectedData, string(data))
			})
		}
	})

	t.Run("invalid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadInitiator strategyJSON.PayloadInitiator
			payloadData      interface{}
			expectedError    error
		}

		testCases := []testCase{
			{
				"not registered convert payload",
				nil,
				&simpleType{Test: "test", Order: 1},
				strategyJSON.ErrPayloadNotRegistered,
			},
			{
				"error marshalling payload",
				func() interface{} {
					// Need to register something that is not json serializable.
					return func() {}
				},
				func() {},
				strategyJSON.ErrPayloadCannotBeSerialized,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := strategyJSON.NewPayloadTransformer()
				if tc.payloadInitiator != nil {
					require.NoError(t,
						transformer.RegisterPayload("tests", tc.payloadInitiator),
					)
				}

				name, data, err := transformer.ConvertPayload(tc.payloadData)
				asserts.Equal(tc.expectedError, err)
				asserts.Equal("", name)
				asserts.Equal([]byte(nil), data)
			})
		}
	})
}

func TestJSONPayloadTransformer_CreatePayload(t *testing.T) {
	t.Run("payload creation", func(t *testing.T) {
		type validTestCase struct {
			title            string
			payloadType      string
			payloadInitiator strategyJSON.PayloadInitiator
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

				factory := strategyJSON.NewPayloadTransformer()
				err := factory.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)
				require.NoError(t, err)

				res, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				asserts.EqualValues(testCase.expectedData, res)
				asserts.NoError(err)
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
				strategyJSON.ErrUnsupportedJSONPayloadData,
			},
			{
				"unknown payload type",
				"test",
				[]byte{},
				strategyJSON.ErrUnknownPayloadType,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := strategyJSON.NewPayloadTransformer()
				res, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				assert.Equal(t, testCase.expectedError, err)
				assert.Nil(t, res)
			})
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadInitiator strategyJSON.PayloadInitiator
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
				factory := strategyJSON.NewPayloadTransformer()
				require.NoError(t,
					factory.RegisterPayload("tests", testCase.payloadInitiator),
				)

				res, err := factory.CreatePayload("tests", testCase.payloadData)

				assert.IsType(t, (*json.SyntaxError)(nil), err)
				assert.Nil(t, res)
			})
		}
	})
}

func TestJSONPayloadTransformer_RegisterPayload(t *testing.T) {
	t.Run("register a type", func(t *testing.T) {
		transformer := strategyJSON.NewPayloadTransformer()
		err := transformer.RegisterPayload("test", func() interface{} {
			return &struct{ order int }{}
		})

		assert.Nil(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := transformer.RegisterPayload("test", func() interface{} {
				return &struct{ order int }{}
			})

			assert.Equal(t, strategyJSON.ErrDuplicatePayloadType, err)
		})
	})

	t.Run("failed registrations", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadType      string
			payloadInitiator strategyJSON.PayloadInitiator
			expectedError    error
		}

		testCases := []invalidTestCase{
			{
				"nil initiator",
				"nil",
				func() interface{} {
					return nil
				},
				strategyJSON.ErrInitiatorInvalidResult,
			},
			{
				"nil reference initiator",
				"nil",
				func() interface{} {
					return (*invalidTestCase)(nil)
				},
				strategyJSON.ErrInitiatorInvalidResult,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				transformer := strategyJSON.NewPayloadTransformer()
				err := transformer.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)

				assert.Equal(t, testCase.expectedError, err)
			})
		}
	})
}

func TestPayloadTransformer_RegisterMultiplePayloads(t *testing.T) {
	t.Run("register multiple types", func(t *testing.T) {
		transformer := strategyJSON.NewPayloadTransformer()
		err := transformer.RegisterPayloads(map[string]strategyJSON.PayloadInitiator{
			"order": func() interface{} {
				return &struct{ order int }{}
			},
			"box": func() interface{} {
				return &struct{ box int }{}
			},
		})

		assert.NoError(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := transformer.RegisterPayloads(map[string]strategyJSON.PayloadInitiator{
				"order": func() interface{} {
					return &struct{ order int }{}
				},
			})

			assert.Equal(t, strategyJSON.ErrDuplicatePayloadType, err)
		})
	})
}
