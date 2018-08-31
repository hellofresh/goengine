// +build unit

package inmemory_test

import (
	"testing"

	"github.com/hellofresh/goengine/eventstore/inmemory"
	anotherpayload "github.com/hellofresh/goengine/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/internal/mocks/payload"
	"github.com/stretchr/testify/assert"
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
			anotherpayload.Payload{},
		},
	}

	registry := &inmemory.PayloadRegistry{}
	if !assert.NotNil(t, registry) {
		return
	}

	t.Run("register payloads", func(t *testing.T) {
		asserts := assert.New(t)
		for _, testCase := range testCases {
			err := registry.RegisterPayload(testCase.payloadType, testCase.payload)

			asserts.NoError(err)
		}
	})

	t.Run("resolve payloads", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.title, func(t *testing.T) {
				name, err := registry.ResolveEventName(tc.payload)

				asserts := assert.New(t)
				if asserts.NoError(err) {
					asserts.Equal(tc.payloadType, name)
				}
			})
		}
	})

	t.Run("duplicate type registry", func(t *testing.T) {
		asserts := assert.New(t)
		for _, testCase := range testCases {
			err := registry.RegisterPayload(testCase.payloadType, testCase.payload)

			if asserts.Error(err) {
				asserts.Equal(inmemory.ErrDuplicatePayloadType, err)
			}
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		name, err := registry.ResolveEventName(struct{}{})

		asserts := assert.New(t)
		if asserts.Error(err) {
			asserts.Equal(inmemory.ErrUnknownPayloadType, err)
		}
		asserts.Empty(name)
	})
}
