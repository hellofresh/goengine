// +build unit

package logrus_test

import (
	"testing"

	"github.com/hellofresh/goengine/extension/logrus"
)

func BenchmarkStandardLoggerEntry(b *testing.B) {
	b.ReportAllocs()

	logger := logrus.StandardLogger()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		logger.WithField("i", n).Debug("test")
	}
}
