package metadata_test

import (
	"testing"

	"time"

	"github.com/hellofresh/goengine/metadata"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	asserts := assert.New(t)

	m1 := metadata.New()
	asserts.NotNil(m1)

	m2 := metadata.New()
	asserts.NotNil(m2)

	asserts.False(m1 == m2, "New instances should not be identical")
}

func TestWithValue(t *testing.T) {
	t.Run("with instance", func(t *testing.T) {
		asserts := assert.New(t)

		m := metadata.New()
		asserts.Nil(m.Value(""))

		m = metadata.WithValue(m, "", "empty_string")
		asserts.Equal("empty_string", m.Value(""))

		m = metadata.WithValue(m, "second", time.Second)
		asserts.Equal(time.Second, m.Value("second"))
		asserts.Equal("empty_string", m.Value(""))
	})

	t.Run("with nil", func(t *testing.T) {
		var m metadata.Metadata
		m = metadata.WithValue(m, "key", "empty_string")

		assert.Equal(t, "empty_string", m.Value("key"))
	})
}

func TestAsMap(t *testing.T) {
	t.Run("empty metadata", func(t *testing.T) {
		mMap := metadata.New().AsMap()

		assert.Len(t, mMap, 0)
	})

	t.Run("nil parent metadata", func(t *testing.T) {
		expectedMap := map[string]interface{}{
			"test": 1.1,
		}

		var m metadata.Metadata
		m = metadata.WithValue(m, "test", 1.1)

		mMap := m.AsMap()

		assert.Len(t, mMap, 1)
		assert.Equal(t, expectedMap, mMap)
	})

	t.Run("metadata with multiple values", func(t *testing.T) {
		expectedMap := map[string]interface{}{
			"test":    nil,
			"another": "value",
		}

		m := metadata.New()
		m = metadata.WithValue(m, "test", nil)
		m = metadata.WithValue(m, "another", "value")

		mMap := m.AsMap()

		assert.Len(t, mMap, 2)
		assert.Equal(t, expectedMap, mMap)
	})

	t.Run("metadata with overridden values", func(t *testing.T) {
		expectedMap := map[string]interface{}{
			"test":    nil,
			"another": "another_value",
		}

		m := metadata.New()
		m = metadata.WithValue(m, "test", nil)
		m = metadata.WithValue(m, "another", "value")
		m = metadata.WithValue(m, "another", "another_value")

		mMap := m.AsMap()

		assert.Len(t, mMap, 2)
		assert.Equal(t, expectedMap, mMap)
	})
}
