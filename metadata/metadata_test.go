// +build unit

package metadata_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode"

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

func TestFromMap(t *testing.T) {
	type mapTestCase struct {
		title string
		input map[string]interface{}
	}

	testCases := []mapTestCase{
		{
			"map with data",
			map[string]interface{}{
				"v":        1,
				"foo":      "bar",
				"is_valid": true,
			},
		},
		{
			"empty map",
			map[string]interface{}{},
		},
		{
			"nil map",
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			expectedMap := testCase.input
			if expectedMap == nil {
				expectedMap = map[string]interface{}{}
			}

			m := metadata.FromMap(testCase.input)

			assert.Equal(t, expectedMap, m.AsMap())
		})
	}
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
	{
		"metadata with 'complex' json",
		func() metadata.Metadata {
			m := metadata.New()
			m = metadata.WithValue(m, "test", nil)
			m = metadata.WithValue(m, "another", "value")
			m = metadata.WithValue(m, "arr", []interface{}{"a", "b", "c"})
			m = metadata.WithValue(m, "arrInArr", []interface{}{"a", []interface{}{"b"}})
			m = metadata.WithValue(m, "obj", map[string]interface{}{"a": float64(1)})
			m = metadata.WithValue(m, "objInObj", map[string]interface{}{
				"a": float64(1),
				"b": map[string]interface{}{"a": float64(2)},
			})
			return m
		},
		`{
			"test": null,
			"another": "value",
			"arr": [ "a", "b", "c" ],
			"arrInArr": [ "a", [ "b" ] ],
			"obj": { "a": 1 },
			"objInObj": { "a": 1, "b": {"a": 2} }
		}`,
	},
}

func TestMarshalJSON(t *testing.T) {
	for _, testCase := range jsonTestCases {
		t.Run(testCase.title, func(t *testing.T) {
			expectedJSON := strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return -1
				}
				return r
			}, testCase.json)

			m := testCase.metadata()

			mJSON, err := json.Marshal(m)

			assert.Equal(t, expectedJSON, string(mJSON))
			assert.NoError(t, err)
		})
	}
}

func TestUnmarshalJSON(t *testing.T) {
	for _, testCase := range jsonTestCases {
		t.Run(testCase.title, func(t *testing.T) {
			m, err := metadata.UnmarshalJSON([]byte(testCase.json))

			// Need to use AsMap otherwise we can have inconsistent tests results.
			if assert.NoError(t, err) {
				assert.Equal(t, testCase.metadata(), m)
			}
		})
	}
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	payload := []byte(`{"_aggregate_id": "b9ebca7a-c1eb-40dd-94a4-fac7c5e84fb5", "_aggregate_type": "bank_account", "_aggregate_version": 1}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := metadata.UnmarshalJSON(payload)
		if err != nil {
			b.Fail()
		}
	}
}

func BenchmarkMarshalJSON(b *testing.B) {
	m := metadata.New()
	m = metadata.WithValue(m, "_aggregate_id", "b9ebca7a-c1eb-40dd-94a4-fac7c5e84fb5")
	m = metadata.WithValue(m, "_aggregate_type", "bank_account")
	m = metadata.WithValue(m, "_aggregate_version", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(m)
		if err != nil {
			b.Fail()
		}
	}
}
