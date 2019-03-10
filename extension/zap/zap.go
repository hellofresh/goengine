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

func (w wrapper) Error(msg string, fields func(goengine.LoggerEntry)) {
	if ce := w.logger.Check(zap.ErrorLevel, msg); ce != nil {
		ce.Write(fieldsToZapFields(fields)...)
	}
}

func (w wrapper) Warn(msg string, fields func(goengine.LoggerEntry)) {
	if ce := w.logger.Check(zap.WarnLevel, msg); ce != nil {
		ce.Write(fieldsToZapFields(fields)...)
	}
}

func (w wrapper) Info(msg string, fields func(goengine.LoggerEntry)) {
	if ce := w.logger.Check(zap.InfoLevel, msg); ce != nil {
		ce.Write(fieldsToZapFields(fields)...)
	}
}

func (w wrapper) Debug(msg string, fields func(goengine.LoggerEntry)) {
	if ce := w.logger.Check(zap.DebugLevel, msg); ce != nil {
		ce.Write(fieldsToZapFields(fields)...)
	}
}

func (w *wrapper) WithFields(fields func(goengine.LoggerEntry)) goengine.Logger {
	if fields == nil {
		return w
	}

	return &wrapper{logger: w.logger.With(fieldsToZapFields(fields)...)}
}

func fieldsToZapFields(fields func(goengine.LoggerEntry)) []zap.Field {
	if fields == nil {
		return make([]zap.Field, 0)
	}

	// TODO use some sort of pool
	e := &entry{fields: []zap.Field{}}
	fields(e)
	return e.fields
}

type entry struct {
	fields []zap.Field
}

func (e *entry) Int(k string, v int) {
	e.fields = append(e.fields, zap.Int(k, v))
}

func (e *entry) String(k, v string) {
	e.fields = append(e.fields, zap.String(k, v))
}

func (e *entry) Error(err error) {
	e.fields = append(e.fields, zap.Error(err))
}

func (e *entry) Any(k string, v interface{}) {
	e.fields = append(e.fields, zap.Any(k, v))
}
