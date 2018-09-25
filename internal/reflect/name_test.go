// +build unit

package reflect_test

import (
	"testing"

	anotherpayload "github.com/hellofresh/goengine/internal/mocks/another/payload"
	"github.com/hellofresh/goengine/internal/mocks/payload"
	"github.com/hellofresh/goengine/internal/reflect"
	"github.com/stretchr/testify/assert"
)

func TestFullTypeNameOf(t *testing.T) {
	testCases := []struct {
		title        string
		expectedName string
		obj          interface{}
	}{
		{
			"payload",
			"github.com/hellofresh/goengine/internal/mocks/payload.Payload",
			payload.Payload{},
		},
		{
			"another payload",
			"github.com/hellofresh/goengine/internal/mocks/another/payload.Payload",
			anotherpayload.Payload{},
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
