package metadata_test

import (
	"encoding/json"
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
	type mapTestCase struct {
		title       string
		setup       func() metadata.Metadata
		expectedMap map[string]interface{}
	}

	testCases := []mapTestCase{
		{
			"empty metadata",
			func() metadata.Metadata {
				return metadata.New()
			},
			map[string]interface{}{},
		},
		{
			"nil parent metadata",
			func() metadata.Metadata {
				var m metadata.Metadata
				return metadata.WithValue(m, "test", 1.1)
			},
			map[string]interface{}{
				"test": 1.1,
			},
		},
		{
			"metadata with multiple values",
			func() metadata.Metadata {
				m := metadata.New()
				m = metadata.WithValue(m, "test", nil)
				return metadata.WithValue(m, "another", "value")
			},
			map[string]interface{}{
				"test":    nil,
				"another": "value",
			},
		},
		{
			"metadata with overridden values",
			func() metadata.Metadata {
				m := metadata.New()
				m = metadata.WithValue(m, "test", nil)
				m = metadata.WithValue(m, "another", "value")
				return metadata.WithValue(m, "another", "another_value")
			},
			map[string]interface{}{
				"test":    nil,
				"another": "another_value",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			m := testCase.setup()

			mMap := m.AsMap()

			assert.Equal(t, testCase.expectedMap, mMap)
		})
	}
}

var jsonTestCases = []struct {
	title    string
	metadata func() metadata.Metadata
	json     string
}{
	{
		"empty metadata",
		func() metadata.Metadata {
			return metadata.New()
		},
		`{}`,
	},
	{
		"metadata with multiple values",
		func() metadata.Metadata {
			m := metadata.New()
			m = metadata.WithValue(m, "test", nil)
			return metadata.WithValue(m, "another", "value")
		},
		`{
			"test": null,
			"another": "value"
		}`,
	},
}

func TestMetadata_MarshalJSON(t *testing.T) {
	for _, testCase := range jsonTestCases {
		t.Run(testCase.title, func(t *testing.T) {
			m := testCase.metadata()

			mJSON, err := json.Marshal(m)

			asserts := assert.New(t)
			asserts.JSONEq(testCase.json, string(mJSON))
			asserts.Nil(err)
		})
	}
}

func TestJSONMetadata_MarshalJSON(t *testing.T) {
	for _, testCase := range jsonTestCases {
		t.Run(testCase.title, func(t *testing.T) {
			m := metadata.JSONMetadata{
				Metadata: testCase.metadata(),
			}

			mJSON, err := json.Marshal(m)

			asserts := assert.New(t)
			asserts.JSONEq(testCase.json, string(mJSON))
			asserts.Nil(err)
		})
	}
}

func TestJSONMetadata_UnmarshalJSON(t *testing.T) {
	for _, testCase := range jsonTestCases {
		t.Run(testCase.title, func(t *testing.T) {
			var m metadata.JSONMetadata
			err := json.Unmarshal([]byte(testCase.json), &m)

			asserts := assert.New(t)
			// Need to use AsMap otherwise we can have inconsistent tests results.
			asserts.Equal(testCase.metadata().AsMap(), m.Metadata.AsMap())
			asserts.NoError(err)
		})
	}
}
