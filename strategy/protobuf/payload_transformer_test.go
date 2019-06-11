// +build unit

package protobuf_test

import (
	"testing"

	anotherpayload "github.com/hellofresh/goengine/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/internal/mocks/payload"
	"github.com/hellofresh/goengine/strategy/protobuf"
	"github.com/hellofresh/goengine/strategy/protobuf/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPayloadTransformer(t *testing.T) {
	t.Run("same type on different packages", func(t *testing.T) {
		asserts := assert.New(t)

		transformer := protobuf.NewPayloadTransformer()
		require.NoError(t,
			transformer.RegisterPayload("payload", func() interface{} {
				return payload.Payload{}
			}),
		)

		name, data, err := transformer.ConvertPayload(anotherpayload.Payload{})
		asserts.Equal(err, protobuf.ErrPayloadNotRegistered)
		asserts.Equal("", name)
		asserts.Equal([]byte(nil), data)
	})
}

func TestPayloadTransformer_ConvertPayload(t *testing.T) {

	t.Run("valid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadType      string
			payloadInitiator protobuf.PayloadInitiator
			payloadData      interface{}
			expectedData     []byte
		}

		testCases := []testCase{
			{
				"convert payload",
				"tests",
				func() interface{} {
					return &internal.Simple{}
				},
				&internal.Simple{Test: "test", Order: 1},
				[]byte{0xa, 0x4, 0x74, 0x65, 0x73, 0x74, 0x10, 0x1},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := protobuf.NewPayloadTransformer()
				require.NoError(t,
					transformer.RegisterPayload(tc.payloadType, tc.payloadInitiator),
				)

				name, data, err := transformer.ConvertPayload(tc.payloadData)
				asserts.NoError(err)
				asserts.Equal(tc.payloadType, name)
				asserts.Equal(tc.expectedData, data)
			})
		}
	})

	t.Run("invalid tests", func(t *testing.T) {
		type testCase struct {
			title            string
			payloadInitiator protobuf.PayloadInitiator
			payloadData      interface{}
			expectedError    error
		}

		testCases := []testCase{
			{
				"not registered convert payload",
				nil,
				&internal.Simple{Test: "test", Order: 1},
				protobuf.ErrPayloadNotRegistered,
			},
			{
				"error marshalling payload",
				func() interface{} {
					// Need to register something that is not protobuf serializable.
					return func() {}
				},
				func() {},
				protobuf.ErrNotProtobufPayload,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				asserts := assert.New(t)
				transformer := protobuf.NewPayloadTransformer()
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

func TestProtobufPayloadTransformer_CreatePayload(t *testing.T) {
	t.Run("payload creation", func(t *testing.T) {
		type validTestCase struct {
			title            string
			payloadType      string
			payloadInitiator protobuf.PayloadInitiator
			payloadData      interface{}
			expectedData     interface{}
		}

		testCases := []validTestCase{
			{
				"message",
				"message",
				func() interface{} {
					return internal.Simple{}
				},
				[]byte{0xa, 0x4, 0x74, 0x65, 0x73, 0x74, 0x10, 0x1},
				internal.Simple{Test: "test", Order: 1},
			},
			{
				"message pointer",
				"prt_message",
				func() interface{} {
					return &internal.Simple{}
				},
				[]byte{0xa, 0x4, 0x74, 0x65, 0x73, 0x74, 0x10, 0x1},
				&internal.Simple{Test: "test", Order: 1},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)

				factory := protobuf.NewPayloadTransformer()
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
				protobuf.ErrUnsupportedProtobufPayloadData,
			},
			{
				"unknown payload type",
				"test",
				[]byte{},
				protobuf.ErrUnknownPayloadType,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := protobuf.NewPayloadTransformer()
				res, err := factory.CreatePayload(testCase.payloadType, testCase.payloadData)

				assert.Equal(t, testCase.expectedError, err)
				assert.Nil(t, res)
			})
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadInitiator protobuf.PayloadInitiator
			payloadData      interface{}
		}

		testCases := []invalidTestCase{
			{
				"bad protobuf",
				func() interface{} {
					return internal.Simple{}
				},
				`not protobuf`,
			},
			{
				"bad protobuf for a reference type",
				func() interface{} {
					return &internal.Simple{}
				},
				[]byte(`not protobuf`),
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				factory := protobuf.NewPayloadTransformer()
				require.NoError(t,
					factory.RegisterPayload("tests", testCase.payloadInitiator),
				)

				res, err := factory.CreatePayload("tests", testCase.payloadData)

				assert.Error(t, err)
				assert.Nil(t, res)
			})
		}
	})
}

func TestProtobufPayloadTransformer_RegisterPayload(t *testing.T) {
	t.Run("register a type", func(t *testing.T) {
		transformer := protobuf.NewPayloadTransformer()
		err := transformer.RegisterPayload("test", func() interface{} {
			return &struct{ order int }{}
		})

		assert.Nil(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := transformer.RegisterPayload("test", func() interface{} {
				return &struct{ order int }{}
			})

			assert.Equal(t, protobuf.ErrDuplicatePayloadType, err)
		})
	})

	t.Run("failed registrations", func(t *testing.T) {
		type invalidTestCase struct {
			title            string
			payloadType      string
			payloadInitiator protobuf.PayloadInitiator
			expectedError    error
		}

		testCases := []invalidTestCase{
			{
				"nil initiator",
				"nil",
				func() interface{} {
					return nil
				},
				protobuf.ErrInitiatorInvalidResult,
			},
			{
				"nil reference initiator",
				"nil",
				func() interface{} {
					return (*invalidTestCase)(nil)
				},
				protobuf.ErrInitiatorInvalidResult,
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				transformer := protobuf.NewPayloadTransformer()
				err := transformer.RegisterPayload(testCase.payloadType, testCase.payloadInitiator)

				assert.Equal(t, testCase.expectedError, err)
			})
		}
	})
}

func TestPayloadTransformer_RegisterMultiplePayloads(t *testing.T) {
	t.Run("register multiple types", func(t *testing.T) {
		transformer := protobuf.NewPayloadTransformer()
		err := transformer.RegisterPayloads(map[string]protobuf.PayloadInitiator{
			"order": func() interface{} {
				return &struct{ order int }{}
			},
			"box": func() interface{} {
				return &struct{ box int }{}
			},
		})

		assert.NoError(t, err)

		t.Run("duplicate registration", func(t *testing.T) {
			err := transformer.RegisterPayloads(map[string]protobuf.PayloadInitiator{
				"order": func() interface{} {
					return &struct{ order int }{}
				},
			})

			assert.Equal(t, protobuf.ErrDuplicatePayloadType, err)
		})
	})
}
