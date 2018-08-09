package metadata_test

import (
	"testing"

	"github.com/hellofresh/goengine/metadata"
	"github.com/stretchr/testify/assert"
)

type (
	matchesTestCase struct {
		title   string
		matcher metadata.Matcher
		data    metadata.Metadata
	}

	matchesErrTestCase struct {
		title         string
		matcher       metadata.Matcher
		data          metadata.Metadata
		expectedError error
	}
)

type SpecialString string

func TestMatches(t *testing.T) {
	t.Run("Test valid matches", func(t *testing.T) {
		validTestCases := []matchesTestCase{
			{
				"string equals string",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					"string",
				),
				metadata.WithValue(
					metadata.New(),
					"key",
					"string",
				),
			},
			{
				"string equals specialString",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					"string",
				),
				metadata.WithValue(
					metadata.New(),
					"key",
					SpecialString("string"),
				),
			},
			{
				"specialString equals string",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					SpecialString("string"),
				),
				metadata.WithValue(
					metadata.New(),
					"key",
					"string",
				),
			},
		}

		for _, test := range validTestCases {
			t.Run(test.title, func(t *testing.T) {
				matched, err := metadata.Matches(test.matcher, test.data)

				asserts := assert.New(t)
				asserts.True(matched)
				asserts.Nil(err)
			})
		}
	})

	t.Run("Test invalid matches", func(t *testing.T) {
		invalidTestCases := []matchesTestCase{
			{
				"int not equal to int",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					10,
				),
				metadata.WithValue(
					metadata.New(),
					"key",
					13,
				),
			},
		}

		for _, test := range invalidTestCases {
			t.Run(test.title, func(t *testing.T) {
				matched, err := metadata.Matches(test.matcher, test.data)

				asserts := assert.New(t)
				asserts.False(matched)
				asserts.Nil(err)
			})
		}
	})

	t.Run("Test bad/error matches", func(t *testing.T) {
		errorTestCases := []matchesErrTestCase{
			{
				"int not equal to string",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					10,
				),
				metadata.WithValue(
					metadata.New(),
					"key",
					"10",
				),
				metadata.ErrTypeMismatch,
			},
			{
				"bool cannot be greater then",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"bool",
					metadata.GreaterThan,
					true,
				),
				metadata.WithValue(
					metadata.New(),
					"bool",
					true,
				),
				metadata.ErrUnsupportedOperator,
			},
			{
				"struct is not a scalar",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"struct",
					metadata.Equals,
					struct{}{},
				),
				metadata.WithValue(
					metadata.New(),
					"struct",
					struct{}{},
				),
				metadata.ErrUnsupportedType,
			},
		}

		for _, test := range errorTestCases {
			t.Run(test.title, func(t *testing.T) {
				matched, err := metadata.Matches(test.matcher, test.data)

				asserts := assert.New(t)
				asserts.False(matched)
				asserts.Equal(test.expectedError, err)
			})
		}
	})
}
