//go:build unit

package inmemory_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/v2/driver/inmemory"
	anotherPayload "github.com/hellofresh/goengine/v2/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/v2/internal/mocks/payload"
)

func TestPayloadRegistry(t *testing.T) {
	type testCase struct {
		title       string
		payloadType string
		payload     interface{}
	}

	testCases := []testCase{
		{
			"a payload",
			"payload",
			payload.Payload{},
		},
		{
			"another payload",
			"another_payload",
			anotherPayload.Payload{},
		},
	}

	registry := &inmemory.PayloadRegistry{}

	t.Run("register payloads", func(t *testing.T) {
		for _, testCase := range testCases {
			err := registry.RegisterPayload(testCase.payloadType, testCase.payload)

			assert.NoError(t, err)
		}
	})

	t.Run("resolve payloads", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				name, err := registry.ResolveName(tc.payload)

				assert.Equal(t, tc.payloadType, name)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("duplicate type registry", func(t *testing.T) {
		for _, testCase := range testCases {
			err := registry.RegisterPayload(testCase.payloadType, testCase.payload)

			assert.Equal(t, inmemory.ErrDuplicatePayloadType, err)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		name, err := registry.ResolveName(struct{}{})

		assert.Equal(t, inmemory.ErrUnknownPayloadType, err)
		assert.Empty(t, name)
	})
}
