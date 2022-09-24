//go:build unit

package inmemory_test

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/v2/driver/inmemory"
	log "github.com/hellofresh/goengine/v2/extension/logrus"
	"github.com/hellofresh/goengine/v2/metadata"
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

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, log.Wrap(logger))

				asserts := assert.New(t)
				asserts.NoError(err)
				asserts.IsType((*inmemory.MetadataMatcher)(nil), matcher)
				asserts.Len(loggerHook.Entries, 0)
			})
		}
	})
	t.Run("incompatible metadata.Matcher", func(t *testing.T) {
		type incompatibleMatcherTestCase struct {
			title         string
			matcher       metadata.Matcher
			expectedError string
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
				fmt.Sprintf("constaint bool > true is incompatible %s", inmemory.ErrUnsupportedOperator),
			},
			{
				"unsupported value (struct is not supported)",
				metadata.WithConstraint(
					metadata.NewMatcher(),
					"key",
					metadata.Equals,
					struct{}{},
				),
				fmt.Sprintf("constaint key = {} is incompatible %s", inmemory.ErrUnsupportedType),
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, log.Wrap(logger))

				asserts := assert.New(t)
				asserts.IsType(inmemory.IncompatibleMatcherError{}, err)
				asserts.Equal("goengine: incompatible metadata.Matcher\n"+testCase.expectedError, err.Error())
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
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, log.Wrap(logger))

				asserts := assert.New(t)
				asserts.NoError(err)
				asserts.True(matcher.Matches(testCase.data))
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
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, log.Wrap(logger))

				asserts := assert.New(t)
				asserts.NoError(err)
				asserts.False(matcher.Matches(testCase.data))
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
				logger, loggerHook := test.NewNullLogger()

				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, log.Wrap(logger))

				asserts := assert.New(t)
				asserts.NoError(err)
				asserts.False(matcher.Matches(testCase.data))

				logEntries := loggerHook.AllEntries()
				if asserts.Len(logEntries, 1) {
					entry := logEntries[0]

					asserts.Contains(entry.Data, logrus.ErrorKey)
					asserts.Equal(logrus.WarnLevel, entry.Level)
					asserts.Equal(testCase.expectedError, entry.Data[logrus.ErrorKey])
				}
			})
		}

		for _, testCase := range errorTestCases {
			t.Run(testCase.title+"(without logger)", func(t *testing.T) {
				matcher, err := inmemory.NewMetadataMatcher(testCase.matcher, nil)

				assert.NoError(t, err)
				assert.False(t, matcher.Matches(testCase.data))
			})
		}
	})
}
