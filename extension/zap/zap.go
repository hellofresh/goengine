package zap

import (
	"github.com/hellofresh/goengine"
	"go.uber.org/zap"
)

type wrapper struct {
	logger *zap.Logger
}

// Wrap wraps a zap.Logger
func Wrap(logger *zap.Logger) goengine.Logger {
	return &wrapper{logger}
}

// Error writes a log with log level error
func (w wrapper) Error(msg string) {
	w.logger.Error(msg)
}

// Warn writes a log with log level warning
func (w wrapper) Warn(msg string) {
	w.logger.Warn(msg)
}

// Info writes a log with log level info
func (w wrapper) Info(msg string) {
	w.logger.Info(msg)
}

// Debug writes a log with log level debug
func (w wrapper) Debug(msg string) {
	w.logger.Debug(msg)
}

// WithField Adds a field to the log entry
func (w wrapper) WithField(key string, val interface{}) goengine.Logger {
	return wrapper{logger: w.logger.With(zap.Any(key, val))}
}

//WithFields Adds a set of fields to the log entry
func (w wrapper) WithFields(fields goengine.Fields) goengine.Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}

	return wrapper{logger: w.logger.With(zapFields...)}
}

// WithError Add an error as single field to the log entry
func (w wrapper) WithError(err error) goengine.Logger {
	return wrapper{logger: w.logger.With(zap.Error(err))}
}
