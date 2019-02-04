package log

import goengine_dev "github.com/hellofresh/goengine-dev"

// NilLogger is a logger that will not output any logging information
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NilLogger goengine_dev.Logger = &nilLogger{}

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

func (n *nilLogger) WithField(string, interface{}) goengine_dev.Logger {
	return n
}

func (n *nilLogger) WithFields(goengine_dev.Fields) goengine_dev.Logger {
	return n
}

func (n *nilLogger) WithError(error) goengine_dev.Logger {
	return n
}
