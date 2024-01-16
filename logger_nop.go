package goengine

// NopLogger is a no-op Logger.
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NopLogger Logger = &nopLogger{}

// nopLogger is no-op Logger
type nopLogger struct {
}

func (nopLogger) Error(string, func(LoggerEntry)) {
}

func (nopLogger) Warn(string, func(LoggerEntry)) {
}

func (nopLogger) Info(string, func(LoggerEntry)) {
}

func (nopLogger) Debug(string, func(LoggerEntry)) {
}

func (n *nopLogger) WithFields(func(LoggerEntry)) Logger {
	return n
}
