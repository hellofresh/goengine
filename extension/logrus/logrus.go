package logrus

import (
	"sync/atomic"

	"github.com/hellofresh/goengine"
	"github.com/sirupsen/logrus"
)

type wrapper struct {
	entry *logrus.Entry
}

// Wrap wraps a logrus.Logger
func Wrap(logger *logrus.Logger) goengine.Logger {
	return &wrapper{entry: logrus.NewEntry(logger)}
}

// WrapEntry wraps a logrus.Entry
func WrapEntry(entry *logrus.Entry) goengine.Logger {
	return &wrapper{entry: entry}
}

// StandardLogger return a wrapped version of the logrus.StandardLogger()
func StandardLogger() goengine.Logger {
	var origLogger = logrus.StandardLogger()
	return &wrapper{entry: logrus.NewEntry(origLogger)}
}

func (w wrapper) Error(msg string, fields func(goengine.LoggerEntry)) {
	if fields == nil {
		w.entry.Error(msg)
		return
	}

	if w.enabled(logrus.ErrorLevel) {
		w.entry.WithFields(fieldsToMap(fields)).Error(msg)
	}
}

func (w wrapper) Warn(msg string, fields func(goengine.LoggerEntry)) {
	if fields == nil {
		w.entry.Warn(msg)
		return
	}

	if w.enabled(logrus.WarnLevel) {
		w.entry.WithFields(fieldsToMap(fields)).Warn(msg)
	}
}

func (w wrapper) Info(msg string, fields func(goengine.LoggerEntry)) {
	if fields == nil {
		w.entry.Info(msg)
		return
	}

	if w.enabled(logrus.InfoLevel) {
		w.entry.WithFields(fieldsToMap(fields)).Info(msg)
	}
}

func (w wrapper) Debug(msg string, fields func(goengine.LoggerEntry)) {
	if fields == nil {
		w.entry.Debug(msg)
		return
	}

	if w.enabled(logrus.DebugLevel) {
		w.entry.WithFields(fieldsToMap(fields)).Debug(msg)
	}
}

func (w *wrapper) WithFields(fields func(goengine.LoggerEntry)) goengine.Logger {
	if fields == nil {
		return w
	}

	return &wrapper{entry: w.entry.WithFields(fieldsToMap(fields))}
}

func (w wrapper) enabled(level logrus.Level) bool {
	return logrus.Level(atomic.LoadUint32((*uint32)(&w.entry.Logger.Level))) >= level
}

func fieldsToMap(fields func(goengine.LoggerEntry)) logrus.Fields {
	e := entry{}
	fields(e)
	return logrus.Fields(e)
}

type entry map[string]interface{}

func (e entry) Int(k string, v int) {
	e[k] = v
}

func (e entry) String(k, v string) {
	e[k] = v
}

func (e entry) Error(err error) {
	e["error"] = err
}

func (e entry) Any(k string, v interface{}) {
	e[k] = v
}
