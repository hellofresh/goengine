package goengine_dev

// NopLogger is a no-op Logger.
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NopLogger Logger = &nopLogger{}

// nopLogger is no-op Logger
type nopLogger struct {
}

func (n *nopLogger) Error(string) {
}

func (*nopLogger) Warn(string) {
}

func (*nopLogger) Info(string) {
}

func (*nopLogger) Debug(string) {
}

func (n *nopLogger) WithField(string, interface{}) Logger {
	return n
}

func (n *nopLogger) WithFields(Fields) Logger {
	return n
}

func (n *nopLogger) WithError(error) Logger {
	return n
}
