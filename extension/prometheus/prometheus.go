package prometheus

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"time"
)

const namespace = "goengine"

// Metrics is an object for exposing prometheus metrics
type Metrics struct {
	notificationCounter            *prometheus.CounterVec
	notificationQueueDuration      *prometheus.HistogramVec
	notificationProcessingDuration *prometheus.HistogramVec
	notificationStartTimes         map[string]time.Time
}

// NewMetrics instantiate and return an object of Metrics
func NewMetrics() *Metrics {
	return &Metrics{
		// notificationCounter is used to expose 'notification_count' metric
		notificationCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "notification_count",
				Help:      "counter for number of notifications received",
			},
			[]string{"is_notification"},
		),
		// queueDuration is used to expose 'queue_duration_seconds' metrics
		notificationQueueDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "queue_duration_seconds",
				Help:      "histogram of queue latencies",
				Buckets:   []float64{0.1, 0.5, 0.9, 0.99}, //buckets for histogram
			},
			[]string{"retry", "success"},
		),

		// notificationProcessingDuration is used to expose 'notification_handle_duration_seconds' metrics
		notificationProcessingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "notification_processing_duration_seconds",
				Help:      "histogram of notifications handled latencies",
				Buckets:   []float64{0.1, 0.5, 0.9, 0.99}, //buckets for histogram
			},
			[]string{"success", "retry"},
		),
	}
}

// RegisterMetrics returns http handler for prometheus
func (m *Metrics) RegisterMetrics(registry *prometheus.Registry) error {
	err := registry.Register(m.notificationCounter)
	if err != nil {
		return err
	}

	err = registry.Register(m.notificationQueueDuration)
	if err != nil {
		return err
	}

	return registry.Register(m.notificationProcessingDuration)
}

// ReceivedNotification counts received notifications
func (m *Metrics) ReceivedNotification(isNotification bool) {
	labels := prometheus.Labels{"is_notification": strconv.FormatBool(isNotification)}
	m.notificationCounter.With(labels).Inc()
}

// QueueNotification returns http handler for prometheus
func (m *Metrics) QueueNotification(notification interface{}) {
	key := "q" + fmt.Sprintf("%p", notification)
	m.notificationStartTimes[key] = time.Now()
}

// StartNotificationProcessing is used to record start time of notification processing
func (m *Metrics) StartNotificationProcessing(notification interface{}) {
	key := "p" + fmt.Sprintf("%p", notification)
	m.notificationStartTimes[key] = time.Now()
}

// FinishNotificationProcessing is used to observe end time of notification queue and processing time
func (m *Metrics) FinishNotificationProcessing(notification interface{}, success bool, retry bool) {
	memAddress := fmt.Sprintf("%p", notification)
	queueStartTime := m.notificationStartTimes["q"+memAddress]
	processingStartTime := m.notificationStartTimes["p"+memAddress]
	labels := prometheus.Labels{"success": strconv.FormatBool(success), "retry": strconv.FormatBool(retry)}

	m.notificationQueueDuration.With(labels).Observe(time.Since(queueStartTime).Seconds())
	m.notificationProcessingDuration.With(labels).Observe(time.Since(processingStartTime).Seconds())
}
