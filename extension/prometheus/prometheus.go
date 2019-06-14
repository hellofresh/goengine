package prometheus

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hellofresh/goengine"

	"github.com/hellofresh/goengine/driver/sql"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace                       = "goengine"
	notificationQueueKeyPrefix      = "q"
	notificationProcessingKeyPrefix = "p"
)

// Metrics is an object for exposing prometheus metrics
type Metrics struct {
	notificationCounter            *prometheus.CounterVec
	notificationQueueDuration      *prometheus.HistogramVec
	notificationProcessingDuration *prometheus.HistogramVec
	notificationStartTimes         sync.Map
	logger                         goengine.Logger
}

// NewMetrics instantiate and return an object of Metrics
func NewMetrics(logger goengine.Logger) *Metrics {
	if logger == nil {
		logger = goengine.NopLogger
	}
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
			[]string{"success"},
		),

		// notificationProcessingDuration is used to expose 'notification_handle_duration_seconds' metrics
		notificationProcessingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "notification_processing_duration_seconds",
				Help:      "histogram of notifications handled latencies",
				Buckets:   []float64{0.1, 0.5, 0.9, 0.99}, //buckets for histogram
			},
			[]string{"success"},
		),
		logger: logger,
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
func (m *Metrics) QueueNotification(notification *sql.ProjectionNotification) {
	if !m.storeStartTime(notificationQueueKeyPrefix, notification) {
		m.logger.Warn("notification already queued", func(e goengine.LoggerEntry) {
			e.Any("notification", notification)
		})
	}

}

// StartNotificationProcessing is used to record start time of notification processing
func (m *Metrics) StartNotificationProcessing(notification *sql.ProjectionNotification) {
	if !m.storeStartTime(notificationProcessingKeyPrefix, notification) {
		m.logger.Warn("notification processing already started", func(e goengine.LoggerEntry) {
			e.Any("notification", notification)
		})
	}
}

// FinishNotificationProcessing is used to observe end time of notification queue and processing time
func (m *Metrics) FinishNotificationProcessing(notification *sql.ProjectionNotification, success bool) {
	memAddress := fmt.Sprintf("%p", notification)
	labels := prometheus.Labels{"success": strconv.FormatBool(success)}

	if queueStartTime, ok := m.notificationStartTimes.Load(notificationQueueKeyPrefix + memAddress); ok {
		m.notificationQueueDuration.With(labels).Observe(time.Since(queueStartTime.(time.Time)).Seconds())
		m.notificationStartTimes.Delete(notificationQueueKeyPrefix + memAddress)

	} else {

		m.logger.Warn("notification queue start time not found", func(e goengine.LoggerEntry) {
			e.Any("notification", notification)
		})
	}

	if processingStartTime, ok := m.notificationStartTimes.Load(notificationProcessingKeyPrefix + memAddress); ok {
		m.notificationProcessingDuration.With(labels).Observe(time.Since(processingStartTime.(time.Time)).Seconds())
		m.notificationStartTimes.Delete(notificationProcessingKeyPrefix + memAddress)
	} else {
		m.logger.Warn("notification processing start time not found", func(e goengine.LoggerEntry) {
			e.Any("notification", notification)
		})
	}
}

// storeStartTime stores the start time against each notification only if it's not already existent
func (m *Metrics) storeStartTime(prefix string, notification *sql.ProjectionNotification) bool {
	key := prefix + fmt.Sprintf("%p", notification)

	_, alreadyExists := m.notificationStartTimes.LoadOrStore(key, time.Now())
	return !alreadyExists
}
