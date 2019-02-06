// +build unit

package zap_test

import (
	"testing"

	zapExtension "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func BenchmarkStandardLoggerEntry(b *testing.B) {
	b.ReportAllocs()

	zapLogger := zap.NewNop()
	logger := zapExtension.Wrap(zapLogger)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		logger.WithField("i", n).Debug("test")
	}
}
