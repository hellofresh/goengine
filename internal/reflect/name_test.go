//go:build unit

package reflect_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	anotherPayload "github.com/hellofresh/goengine/v2/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/v2/internal/mocks/payload"
	"github.com/hellofresh/goengine/v2/internal/reflect"
)

func TestFullTypeNameOf(t *testing.T) {
	testCases := []struct {
		title        string
		expectedName string
		obj          interface{}
	}{
		{
			"payload",
			"github.com/hellofresh/goengine/v2/internal/mocks/payload.Payload",
			payload.Payload{},
		},
		{
			"another payload",
			"github.com/hellofresh/goengine/v2/internal/mocks/another/payload.Payload",
			anotherPayload.Payload{},
		},
		{
			"string",
			"string",
			"test",
		},
		{
			"anonymous",
			"",
			struct{}{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			name := reflect.FullTypeNameOf(testCase.obj)

			assert.Equal(t, testCase.expectedName, name)
		})
	}
}
