package log

type (
	// Logger a structured logger interface
	Logger interface {
		Error(msg string)
		Warn(msg string)
		Info(msg string)
		Debug(msg string)

		WithField(key string, val interface{}) Logger
		WithFields(fields Fields) Logger
		WithError(err error) Logger
	}

	// Fields a map of context provided to the logger
	Fields map[string]interface{}
)
