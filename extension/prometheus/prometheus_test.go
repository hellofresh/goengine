// +build unit

package prometheus_test

import (
	"testing"

	"github.com/hellofresh/goengine/.vendor-new/github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/.vendor-new/github.com/stretchr/testify/require"

	"github.com/hellofresh/goengine/driver/sql"
	goenginePrometheus "github.com/hellofresh/goengine/extension/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_QueueAndFinishNotification(t *testing.T) {
	notification := &sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics()
	require.NoError(t, metrics.RegisterMetrics(registry))

	metrics.QueueNotification(notification)
	metrics.FinishNotificationProcessing(notification, true)

	assertMetricsWhereCalled(t, registry, map[string]uint64{
		"goengine_queue_duration_seconds": 1,
	})
}

func TestMetrics_ReceivedNotification(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()

	metrics := goenginePrometheus.NewMetrics()
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

	require.Len(t, got, len(metricsCounts))
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

//// MockObserver is the mock object for Observer
//type MockObserver struct {
//	observation float64
//}
//
//func (o *MockObserver) Observe(value float64) {
//	o.observation = value
//}
//
//// MockedMetricObject is the mock object for ObserverVec
//type MockedMetricObject struct {
//	observer *MockObserver
//}
//
//func (m *MockedMetricObject) GetMetricWith(labels prometheus.Labels) (prometheus.Observer, error) {
//	return &MockObserver{}, nil
//}
//
//func (m *MockedMetricObject) GetMetricWithLabelValues(lvs ...string) (prometheus.Observer, error) {
//	return &MockObserver{}, nil
//}
//
//func (m *MockedMetricObject) With(labels prometheus.Labels) prometheus.Observer {
//	m.observer = &MockObserver{
//		observation: 0.0,
//	}
//	return m.observer
//}
//
//func (m *MockedMetricObject) WithLabelValues(lvs ...string) prometheus.Observer {
//	return &MockObserver{}
//}
//
//func (m *MockedMetricObject) CurryWith(labels prometheus.Labels) (prometheus.ObserverVec, error) {
//	return m, nil
//}
//
//func (m *MockedMetricObject) MustCurryWith(labels prometheus.Labels) prometheus.ObserverVec {
//	return m
//}
//
//func (m *MockedMetricObject) Describe(ch chan<- *prometheus.Desc) {}
//
//func (m *MockedMetricObject) Collect(ch chan<- prometheus.Metric) {}
//
//func TestMetrics_NotificationStartTime(t *testing.T) {
//	metrics := NewMetrics()
//
//	metrics.QueueNotification(&testSQLProjection)
//	metrics.StartNotificationProcessing(&testSQLProjection)
//
//	memAddress := fmt.Sprintf("%p", &testSQLProjection)
//	queueStartTime, ok := metrics.notificationStartTimes.Load("q" + memAddress)
//	assert.True(t, ok)
//	assert.IsType(t, time.Time{}, queueStartTime)
//
//	processingStartTime, _ := metrics.notificationStartTimes.Load("p" + memAddress)
//	assert.True(t, ok)
//	assert.IsType(t, time.Time{}, processingStartTime)
//
//}
//
//func TestMetrics_FinishNotificationProcessingSuccess(t *testing.T) {
//
//	mockQueueMetric := new(MockedMetricObject)
//	mockProcessMetric := new(MockedMetricObject)
//	testMetrics := newMetricsWith(mockQueueMetric, mockProcessMetric)
//
//	testMetrics.QueueNotification(&testSQLProjection)
//	testMetrics.StartNotificationProcessing(&testSQLProjection)
//	testMetrics.FinishNotificationProcessing(&testSQLProjection, true)
//
//	assert.NotEqual(t, mockQueueMetric.observer.observation, 0.0)
//
//}
//
//func TestMetrics_FinishNotificationProcessingFailureForQueue(t *testing.T) {
//
//	mockQueueMetric := new(MockedMetricObject)
//	mockProcessMetric := new(MockedMetricObject)
//	testMetrics := newMetricsWith(mockQueueMetric, mockProcessMetric)
//
//	testMetrics.StartNotificationProcessing(&testSQLProjection)
//	testMetrics.FinishNotificationProcessing(&testSQLProjection, true)
//
//	assert.Nil(t, mockQueueMetric.observer)
//
//}
//
//func TestMetrics_FinishNotificationProcessingFailureForProcessing(t *testing.T) {
//
//	mockQueueMetric := new(MockedMetricObject)
//	mockProcessMetric := new(MockedMetricObject)
//	testMetrics := newMetricsWith(mockQueueMetric, mockProcessMetric)
//
//	testMetrics.QueueNotification(&testSQLProjection)
//	testMetrics.FinishNotificationProcessing(&testSQLProjection, true)
//
//	assert.Nil(t, mockProcessMetric.observer)
//}
//
//func TestMetrics_CollectAndCompareHistogramMetrics(t *testing.T) {
//
//	registry := prometheus.NewPedanticRegistry()
//	metrics := goenginePrometheus.NewMetrics()
//	metrics.RegisterMetrics(registry)
//
//	metrics.SetLogger(logrus.StandardLogger())
//
//	inputs := []struct {
//		name        string
//		collector   prometheus.ObserverVec
//		metadata    string
//		expect      string
//		observation float64
//	}{
//		{
//			name:      "Testing Queue Duration Metric Collector",
//			collector: metrics.notificationQueueDuration,
//			metadata: `
//				# HELP goengine_queue_duration_seconds histogram of queue latencies
//				# TYPE goengine_queue_duration_seconds histogram
//			`,
//			expect: `
//				goengine_queue_duration_seconds_bucket{success="true",le="0.1"} 0
//				goengine_queue_duration_seconds_bucket{success="true",le="0.5"} 0
//				goengine_queue_duration_seconds_bucket{success="true",le="0.9"} 0
//				goengine_queue_duration_seconds_bucket{success="true",le="0.99"} 1.0
//				goengine_queue_duration_seconds_bucket{success="true",le="+Inf"} 1.0
//				goengine_queue_duration_seconds_sum{success="true"} 0.99
//				goengine_queue_duration_seconds_count{success="true"} 1.0
//
//			`,
//			observation: 0.99,
//		},
//		{
//			name:      "Testing Notification Processing Duration Metric Collector",
//			collector: metrics.notificationProcessingDuration,
//			metadata: `
//				# HELP goengine_notification_processing_duration_seconds histogram of notifications handled latencies
//				# TYPE goengine_notification_processing_duration_seconds histogram
//					`,
//			expect: `
//				goengine_notification_processing_duration_seconds_bucket{success="true",le="0.1"} 0
//				goengine_notification_processing_duration_seconds_bucket{success="true",le="0.5"} 0
//				goengine_notification_processing_duration_seconds_bucket{success="true",le="0.9"} 1.0
//				goengine_notification_processing_duration_seconds_bucket{success="true",le="0.99"} 1.0
//				goengine_notification_processing_duration_seconds_bucket{success="true",le="+Inf"} 1.0
//				goengine_notification_processing_duration_seconds_sum{success="true"} 0.54
//				goengine_notification_processing_duration_seconds_count{success="true"} 1.0
//
//					`,
//			observation: 0.54,
//		},
//	}
//
//	labels := prometheus.Labels{"success": "true"}
//	for _, input := range inputs {
//		input.collector.With(labels).Observe(input.observation)
//		t.Run(input.name, func(t *testing.T) {
//			if err := testutil.CollectAndCompare(input.collector, strings.NewReader(input.metadata+input.expect)); err != nil {
//				t.Errorf("unexpected collecting result:\n%s", err)
//			}
//		})
//
//	}
//}

//// NewMetrics instantiate and return an object of Metrics
//func newMetricsWith(queueDuration prometheus.ObserverVec, processDuration prometheus.ObserverVec) *Metrics {
//	return &Metrics{
//		// queueDuration is used to expose 'queue_duration_seconds' metrics
//		notificationQueueDuration: queueDuration,
//
//		// notificationProcessingDuration is used to expose 'notification_handle_duration_seconds' metrics
//		notificationProcessingDuration: processDuration,
//		logger:                         goengine.NopLogger,
//	}
//}
