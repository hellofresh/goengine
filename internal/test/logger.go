package test

import (
	"io"
	"strings"
	"testing"
)

var _ io.Writer = &logWriter{}

// NewLogWriter an io.Writer wrapper for pushing log writes to t.Log
func NewLogWriter(t testing.TB) io.Writer {
	return &logWriter{
		t: t,
	}
}

type logWriter struct {
	t testing.TB
}

func (l *logWriter) Write(p []byte) (n int, err error) {
	l.t.Log(strings.TrimSpace(string(p)))

	return len(p), nil
}
