//go:build unit
// +build unit

package zap_test

import (
	"errors"
	"testing"

	"github.com/hellofresh/goengine/v2"
	zapExtension "github.com/hellofresh/goengine/v2/extension/zap"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWrap_LogEntry(t *testing.T) {
	zapCore, logObserver := observer.New(zapcore.DebugLevel)
	logger := zapExtension.Wrap(zap.New(zapCore))

	testCases := []struct {
		msg    string
		fields func(goengine.LoggerEntry)

		expectedMsg     string
		expectedContext map[string]interface{}
	}{
		{
			"test nil fields",
			nil,
			"test nil fields",
			map[string]interface{}{},
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
			map[string]interface{}{
				"test":       "a value",
				"normal_int": int64(99),
				"int_64":     int64(2),
				"error":      "some error",
				"obj": struct {
					test string
				}{test: "test property"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.msg, func(t *testing.T) {
			logger.Error(testCase.msg, testCase.fields)
			logger.Warn(testCase.msg, testCase.fields)
			logger.Info(testCase.msg, testCase.fields)
			logger.Debug(testCase.msg, testCase.fields)

			logs := logObserver.TakeAll()

			if !assert.Len(t, logs, 4) {
				return
			}

			levelOrder := []zapcore.Level{
				zapcore.ErrorLevel,
				zapcore.WarnLevel,
				zapcore.InfoLevel,
				zapcore.DebugLevel,
			}

			for i, level := range levelOrder {
				assert.Equal(t, level, logs[i].Level)
				assert.Equal(t, testCase.expectedMsg, logs[i].Message)
				assert.Equal(t, testCase.expectedContext, logs[i].ContextMap())
			}
		})
	}

	t.Run("Do not log disabled levels", func(t *testing.T) {
		zapCore, logObserver := observer.New(zapcore.InfoLevel)
		logger := zapExtension.Wrap(zap.New(zapCore))

		logger.Debug("should not be logged", func(e goengine.LoggerEntry) {
			t.Error("fields should not be called")
		})

		assert.Equal(t, 0, logObserver.Len())
	})
}

func TestWrapper_WithFields(t *testing.T) {
	zapCore, logObserver := observer.New(zapcore.DebugLevel)
	logger := zapExtension.Wrap(zap.New(zapCore))

	t.Run("With fields", func(t *testing.T) {
		loggerWithFields := logger.WithFields(func(e goengine.LoggerEntry) {
			e.String("with field", "check")
			e.String("val", "default")
		})

		loggerWithFields.Debug("test with nil", nil)
		loggerWithFields.Debug("test with override", func(e goengine.LoggerEntry) {
			e.String("val", "override")
		})

		logs := logObserver.TakeAll()
		if assert.Len(t, logs, 2) {
			assert.Equal(t, "test with nil", logs[0].Message)
			assert.Equal(t, map[string]interface{}{
				"with field": "check",
				"val":        "default",
			}, logs[0].ContextMap())

			assert.Equal(t, "test with override", logs[1].Message)
			assert.Equal(t, map[string]interface{}{
				"with field": "check",
				"val":        "override",
			}, logs[1].ContextMap())
		}
	})

	t.Run("With fields nil", func(t *testing.T) {
		loggerWithFields := logger.WithFields(nil)
		assert.Equal(t, logger, loggerWithFields)
	})
}

func BenchmarkStandardLoggerEntry(b *testing.B) {
	b.ReportAllocs()

	zapLogger := zap.NewNop()
	logger := zapExtension.Wrap(zapLogger)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		logger.Debug("test", func(e goengine.LoggerEntry) {
			e.Int("i", n)
		})
	}
}
