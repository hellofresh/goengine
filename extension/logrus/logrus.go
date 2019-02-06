package logrus

import (
	"github.com/hellofresh/goengine"
	"github.com/sirupsen/logrus"
)

var _ goengine.Logger = &Wrapper{}

// Wrapper a struct that wraps the logrus.FieldLogger in order to implement log.Logger
type Wrapper struct {
	logger logrus.FieldLogger
}

// Wrap wraps a logrus.FieldLogger
func Wrap(logger logrus.FieldLogger) *Wrapper {
	return &Wrapper{logger: logger}
}

// StandardLogger return a wrapped version of the logrus.StandardLogger()
func StandardLogger() *Wrapper {
	return Wrap(logrus.StandardLogger())
}

// Error writes a log with log level error
func (w *Wrapper) Error(msg string) {
	w.logger.Error(msg)
}

// Warn writes a log with log level warn
func (w *Wrapper) Warn(msg string) {
	w.logger.Warn(msg)
}

// Info writes a log with log level info
func (w *Wrapper) Info(msg string) {
	w.logger.Warn(msg)
}

// Debug writes a log with log level debug
func (w *Wrapper) Debug(msg string) {
	w.logger.Debug(msg)
}

// WithError Add an error as single field to the log entry
func (w *Wrapper) WithError(err error) goengine.Logger {
	return Wrap(w.logger.WithError(err))
}

// WithField Adds a field to the log entry
func (w *Wrapper) WithField(key string, val interface{}) goengine.Logger {
	return Wrap(w.logger.WithField(key, val))
}

//WithFields Adds a set of fields to the log entry
func (w *Wrapper) WithFields(fields goengine.Fields) goengine.Logger {
	return Wrap(w.logger.WithFields(logrus.Fields(fields)))
}
