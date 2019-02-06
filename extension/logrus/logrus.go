package logrus

import (
	"github.com/hellofresh/goengine"
	"github.com/sirupsen/logrus"
)

type wrapper struct {
	entry *logrus.Entry
}

// Wrap wraps a logrus.Logger
func Wrap(logger *logrus.Logger) goengine.Logger {
	return wrapper{entry: logrus.NewEntry(logger)}
}

// WrapEntry wraps a logrus.Entry
func WrapEntry(entry *logrus.Entry) goengine.Logger {
	return wrapper{entry: entry}
}

// StandardLogger return a wrapped version of the logrus.StandardLogger()
func StandardLogger() goengine.Logger {
	var origLogger = logrus.StandardLogger()
	return wrapper{entry: logrus.NewEntry(origLogger)}
}

// Error writes a log with log level error
func (w wrapper) Error(msg string) {
	w.entry.Error(msg)
}

// Warn writes a log with log level warn
func (w wrapper) Warn(msg string) {
	w.entry.Warn(msg)
}

// Info writes a log with log level info
func (w wrapper) Info(msg string) {
	w.entry.Warn(msg)
}

// Debug writes a log with log level debug
func (w wrapper) Debug(msg string) {
	w.entry.Debug(msg)
}

// WithError Add an error as single field to the log entry
func (w wrapper) WithError(err error) goengine.Logger {
	return wrapper{entry: w.entry.WithError(err)}
}

// WithField Adds a field to the log entry
func (w wrapper) WithField(key string, val interface{}) goengine.Logger {
	return wrapper{entry: w.entry.WithField(key, val)}
}

//WithFields Adds a set of fields to the log entry
func (w wrapper) WithFields(fields goengine.Fields) goengine.Logger {
	return wrapper{entry: w.entry.WithFields(logrus.Fields(fields))}
}
