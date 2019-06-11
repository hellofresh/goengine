package prometheus

import (
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "goengine"

type Metrics struct {
	notificationCounter        *prometheus.CounterVec
	queueDuration              *prometheus.HistogramVec
	notificationHandleDuration *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		// notificationCounter is used to expose 'notification_count' metric
		notificationCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "notification_count",
				Help:      "counter for number of notifications received",
			},
			[]string{"is_event"},
		),
		// queueDuration is used to expose 'queue_duration_seconds' metrics
		queueDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "queue_duration_seconds",
				Help:      "histogram of queue latencies",
				Buckets:   []float64{0.1, 0.5, 0.9, 0.99}, //buckets for histogram
			},
			[]string{"retry", "success"},
		),

		// notificationHandleDuration is used to expose 'notification_handle_duration_seconds' metrics
		notificationHandleDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "notification_handle_duration_seconds",
				Help:      "histogram of event handled latencies",
				Buckets:   []float64{0.1, 0.5, 0.9, 0.99}, //buckets for histogram
			},
			[]string{"retry", "success"},
		),
	}
}

// RegisterMetrics returns http handler for prometheus
func (m *Metrics) RegisterMetrics(registry *prometheus.Registry) error {
	err := registry.Register(m.notificationCounter)
	if err != nil {
		return err
	}

	err = registry.Register(m.queueDuration)
	if err != nil {
		return err
	}

	return registry.Register(m.notificationHandleDuration)
}

// ReceivedNotification
func (m *Metrics) ReceivedNotification(isNotification bool) {

}

// QueueNotification returns http handler for prometheus
func (m *Metrics) QueueNotification(notification *sql.ProjectionNotification) {

}

// StartNotificationProcessing is used to record start time of notification processing
func (m *Metrics) StartNotificationProcessing(notification *sql.ProjectionNotification) {

}

// FinishNotificationProcessing is used to observe end time of notification queue and processing time
func (m *Metrics) FinishNotificationProcessing(notification *sql.ProjectionNotification) {

}
