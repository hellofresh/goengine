package log

import (
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

// NilLogger is a logger that will not output any logging information
// This logger is used when a logger with the value `nil` is passed and avoids the need for `if logger != nil` everywhere
var NilLogger = nilLogger()

func nilLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Out = ioutil.Discard

	return logger
}
