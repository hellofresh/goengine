package inmemory_test

import (
	"testing"

	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/hellofresh/goengine/metadata"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestNewMetadataMatcher(t *testing.T) {
	t.Run("new instance", func(t *testing.T) {
		type matcherTestCase struct {
			title   string
			matcher metadata.Matcher
		}

		testCases := []matcherTestCase{
			{
				"multiple constraints",
				metadata.WithConstraint(
					metadata.WithConstraint(
						metadata.NewMatcher(),
						"key",
						metadata.Equals,
						"string",
					),
					"another",
					metadata.GreaterThanEquals,
					1,
				),
			},
			{
				"empty metadata.Matcher",
				metadata.NewMatcher(),
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, logger)

				asserts := assert.New(t)
				asserts.Nil(err)
				asserts.IsType((*inmemory.MetadataMatcher)(nil), matcher)
				asserts.Len(loggerHook.Entries, 0)
			})
		}
	})
	t.Run("incompatible metadata.Matcher", func(t *testing.T) {
		type incompatibleMatcherTestCase struct {
			title         string
			matcher       metadata.Matcher
			expectedError inmemory.IncompatibleMatcherError
		}

		testCases := []incompatibleMatcherTestCase{
			{
				"unsupported operator (bool cannot be greater then)",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"bool",
					metadata.GreaterThan,
					true,
				),
				inmemory.IncompatibleMatcherError{
					inmemory.ErrUnsupportedOperator,
				},
			},
			{
				"unsupported value (struct is not supported)",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					struct{}{},
				),
				inmemory.IncompatibleMatcherError{
					inmemory.ErrUnsupportedType,
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, logger)

				asserts := assert.New(t)
				asserts.Equal(testCase.expectedError, err)
				asserts.Nil(matcher)
				asserts.Len(loggerHook.Entries, 0)
			})
		}
	})
}

func TestMetadataMatcher_Matches(t *testing.T) {
	type matchesTestCase struct {
		title   string
		matcher metadata.Matcher
		data    metadata.Metadata
	}

	t.Run("valid", func(t *testing.T) {
		type SpecialString string

		testCases := []matchesTestCase{
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

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, logger)
				if asserts.Nil(err) {
					asserts.True(matcher.Matches(testCase.data))
				}
				asserts.Len(loggerHook.Entries, 0)
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		testCases := []matchesTestCase{
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

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, logger)
				if asserts.Nil(err) {
					asserts.False(matcher.Matches(testCase.data))
				}
				asserts.Len(loggerHook.Entries, 0)
			})
		}
	})

	t.Run("invalid with error", func(t *testing.T) {
		type matchesErrTestCase struct {
			title         string
			matcher       metadata.Matcher
			data          metadata.Metadata
			expectedError error
		}

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
				inmemory.ErrTypeMismatch,
			},
		}

		for _, testCase := range errorTestCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, logger)
				if asserts.Nil(err) {
					asserts.False(matcher.Matches(testCase.data))

					logEntries := loggerHook.AllEntries()
					if asserts.Len(logEntries, 1) && asserts.Contains(logEntries[0].Data, logrus.ErrorKey) {
						entry := logEntries[0]
						asserts.Equal(logrus.WarnLevel, entry.Level)
						asserts.Equal(testCase.expectedError, entry.Data[logrus.ErrorKey])
					}
				}
			})
		}

		for _, testCase := range errorTestCases {
			t.Run(testCase.title+"(without logger)", func(t *testing.T) {
				asserts := assert.New(t)

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, nil)
				if asserts.Nil(err) {
					asserts.False(matcher.Matches(testCase.data))
				}
			})
		}
	})
}
