//go:build unit
// +build unit

package prometheus_test

import (
	"testing"

	"github.com/hellofresh/goengine/v2/driver/sql"
	goenginePrometheus "github.com/hellofresh/goengine/v2/extension/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_QueueAndFinishNotification(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.QueueNotification(notification)
	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_queue_duration_seconds":                   1,
		"goengine_notification_processing_duration_seconds": 0,
	})

}

func TestMetrics_ProcessAndFinishNotification(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.StartNotificationProcessing(notification)
	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_notification_processing_duration_seconds": 1,
		"goengine_queue_duration_seconds":                   0,
	})
}

func TestMetrics_QueueProcessAndFinishNotification(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.QueueNotification(notification)
	metrics.StartNotificationProcessing(notification)
	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_notification_processing_duration_seconds": 1,
		"goengine_queue_duration_seconds":                   1,
	})
}

func TestMetrics_FinishNotificationWithoutStart(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_notification_processing_duration_seconds": 0,
		"goengine_queue_duration_seconds":                   0,
	})
}

func TestMetrics_QueueProcessAndFinishNotificationTwice(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.QueueNotification(notification)
	metrics.StartNotificationProcessing(notification)
	metrics.FinishNotificationProcessing(notification, true)
	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_notification_processing_duration_seconds": 1,
		"goengine_queue_duration_seconds":                   1,
	})
}

func TestMetrics_ReceivedNotification(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics(nil)
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.ReceivedNotification(true)
	metrics.ReceivedNotification(false)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_notification_count": 2,
	})
}

func assertMetricsWhereCalled(t *testing.T, g prometheus.Gatherer, metricsCounts map[string]uint64) {
	got, err := g.Gather()
	require.NoError(t, err)

	for _, m := range got {
		expectedCount, ok := metricsCounts[m.GetName()]
		if !assert.Truef(t, ok, "Unknown metric %s", m.GetName()) {
			continue
		}

		var calls uint64
		for _, mm := range m.GetMetric() {
			if h := mm.GetHistogram(); h != nil {
				calls += h.GetSampleCount()
			} else if c := mm.GetCounter(); c != nil {
				calls += uint64(c.GetValue())
			} else {
				t.Errorf("Only Counter and Histogram are supported")
			}
		}
		assert.Equal(t, expectedCount, calls)
	}
}
