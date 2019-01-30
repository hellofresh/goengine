package log

// NilLogger is a logger that will not output any logging information
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NilLogger Logger = &nilLogger{}

type nilLogger struct {
}

func (n *nilLogger) Error(string) {
}

func (*nilLogger) Warn(string) {
}

func (*nilLogger) Info(string) {
}

func (*nilLogger) Debug(string) {
}

func (n *nilLogger) WithField(string, interface{}) Logger {
	return n
}

func (n *nilLogger) WithFields(Fields) Logger {
	return n
}

func (n *nilLogger) WithError(error) Logger {
	return n
}
