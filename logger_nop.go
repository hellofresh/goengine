package goengine

// NopLogger is a no-op Logger.
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NopLogger Logger = &nopLogger{}

// nopLogger is no-op Logger
type nopLogger struct {
}

func (nopLogger) Error(msg string, fields func(LoggerEntry)) {
}

func (nopLogger) Warn(msg string, fields func(LoggerEntry)) {
}

func (nopLogger) Info(msg string, fields func(LoggerEntry)) {
}

func (nopLogger) Debug(msg string, fields func(LoggerEntry)) {
}

func (n *nopLogger) WithFields(fields func(LoggerEntry)) Logger {
	return n
}
