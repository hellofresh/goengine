package goengine

// NopLogger is a no-op Logger.
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NopLogger Logger = &nopLogger{}

// nopLogger is no-op Logger
type nopLogger struct {
}

func (nopLogger) Error(_ string, _ func(LoggerEntry)) {
}

func (nopLogger) Warn(_ string, _ func(LoggerEntry)) {
}

func (nopLogger) Info(_ string, _ func(LoggerEntry)) {
}

func (nopLogger) Debug(_ string, _ func(LoggerEntry)) {
}

func (n *nopLogger) WithFields(_ func(LoggerEntry)) Logger {
	return n
}
