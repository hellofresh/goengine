package goengine

type (
	// Logger a structured logger interface
	Logger interface {
		Error(msg string, fields func(LoggerEntry))
		Warn(msg string, fields func(LoggerEntry))
		Info(msg string, fields func(LoggerEntry))
		Debug(msg string, fields func(LoggerEntry))

		WithFields(fields func(LoggerEntry)) Logger
	}

	// LoggerEntry represents the entry to be logger.
	// This entry can be enhanced with more date.
	LoggerEntry interface {
		Int(k string, v int)
		Int64(s string, v int64)
		String(k, v string)
		Error(err error)
		Any(k string, v interface{})
	}

	// Fields a map of context provided to the logger
	Fields map[string]interface{}
)
