// +build unit

package postgres

import (
	"fmt"
	"github.com/hellofresh/goengine"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultProjectionStateEncoder(t *testing.T) {
	t.Run("Only accept nil values as valid", func(t *testing.T) {
		res, err := defaultProjectionStateEncoder(nil)

		assert.Equal(t, []byte{'{', '}'}, res)
		assert.NoError(t, err)
	})

	t.Run("Reject any state this not nil", func(t *testing.T) {
		var pointer *goengine.Projection
		testCases := []interface{}{
			struct{}{},
			pointer,
			"",
			0,
		}

		for i, v := range testCases {
			t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
				res, err := defaultProjectionStateEncoder(v)

				assert.Error(t, err)
				assert.Nil(t, res)
			})
		}
	})
}
