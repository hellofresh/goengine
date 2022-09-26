//go:build unit

package logrus_test

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/v2"
	logrusExtension "github.com/hellofresh/goengine/v2/extension/logrus"
)

func TestWrap_LogEntry(t *testing.T) {
	logrusLogger, logObserver := test.NewNullLogger()
	logrusLogger.SetLevel(logrus.DebugLevel)
	logger := logrusExtension.Wrap(logrusLogger)

	testCases := []struct {
		msg    string
		fields func(goengine.LoggerEntry)

		expectedMsg     string
		expectedContext logrus.Fields
	}{
		{
			"test nil fields",
			nil,
			"test nil fields",
			logrus.Fields{},
		},
		{
			"test with fields",
			func(e goengine.LoggerEntry) {
				e.String("test", "a value")
				e.Int("normal_int", 99)
				e.Int64("int_64", 2)
				e.Error(errors.New("some error"))
				e.Any("obj", struct {
					test string
				}{test: "test property"})
			},
			"test with fields",
			logrus.Fields{
				"test":       "a value",
				"normal_int": 99,
				"int_64":     int64(2),
				"error":      errors.New("some error"),
				"obj": struct {
					test string
				}{test: "test property"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.msg, func(t *testing.T) {
			defer logObserver.Reset()

			logger.Error(testCase.msg, testCase.fields)
			logger.Warn(testCase.msg, testCase.fields)
			logger.Info(testCase.msg, testCase.fields)
			logger.Debug(testCase.msg, testCase.fields)

			logs := logObserver.AllEntries()
			if !assert.Len(t, logs, 4) {
				return
			}

			levelOrder := []logrus.Level{
				logrus.ErrorLevel,
				logrus.WarnLevel,
				logrus.InfoLevel,
				logrus.DebugLevel,
			}

			for i, level := range levelOrder {
				assert.Equal(t, level, logs[i].Level)
				assert.Equal(t, testCase.expectedMsg, logs[i].Message)
				assert.Equal(t, testCase.expectedContext, logs[i].Data)
			}
		})
	}

	t.Run("Do not log disabled levels", func(t *testing.T) {
		logrusLogger.SetLevel(logrus.InfoLevel)

		logger.Debug("should not be logged", func(e goengine.LoggerEntry) {
			t.Error("fields should not be called")
		})

		assert.Len(t, logObserver.AllEntries(), 0)
	})
}

func TestWrapper_WithFields(t *testing.T) {
	logrusLogger, logObserver := test.NewNullLogger()
	logrusLogger.SetLevel(logrus.DebugLevel)
	logger := logrusExtension.Wrap(logrusLogger)

	t.Run("With fields", func(t *testing.T) {
		loggerWithFields := logger.WithFields(func(e goengine.LoggerEntry) {
			e.String("with field", "check")
			e.String("val", "default")
		})

		loggerWithFields.Debug("test with nil", nil)
		loggerWithFields.Debug("test with override", func(e goengine.LoggerEntry) {
			e.String("val", "override")
		})

		logs := logObserver.AllEntries()
		if assert.Len(t, logs, 2) {
			assert.Equal(t, "test with nil", logs[0].Message)
			assert.Equal(t, logrus.Fields{
				"with field": "check",
				"val":        "default",
			}, logs[0].Data)

			assert.Equal(t, "test with override", logs[1].Message)
			assert.Equal(t, logrus.Fields{
				"with field": "check",
				"val":        "override",
			}, logs[1].Data)
		}
	})

	t.Run("With fields nil", func(t *testing.T) {
		loggerWithFields := logger.WithFields(nil)
		assert.Equal(t, logger, loggerWithFields)
	})
}

func BenchmarkStandardLoggerEntry(b *testing.B) {
	b.ReportAllocs()

	logger := logrusExtension.StandardLogger()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		logger.Debug("test", func(e goengine.LoggerEntry) {
			e.Int("i", n)
		})
	}
}
