package inmemory

import (
	"context"
	"errors"
	"sync"

	"github.com/hellofresh/goengine/driver/sql"
)

// Ensure the NotificationQueue implements sql.NotificationQueuer
var _ sql.NotificationQueuer = &NotificationQueue{}

// NotificationQueue implements a smart queue
type NotificationQueue struct {
	sync.Mutex

	metrics     sql.Metrics
	done        chan struct{}
	queue       chan *sql.ProjectionNotification
	queueBuffer int
}

func NewNotificationQueue(queueBuffer int, metrics sql.Metrics) *NotificationQueue {
	return &NotificationQueue{
		metrics:     metrics,
		queueBuffer: queueBuffer,
	}
}

// Open enables the queue for business
func (nq *NotificationQueue) Open() func() {
	nq.done = make(chan struct{})
	nq.queue = make(chan *sql.ProjectionNotification, nq.queueBuffer)

	return func() {
		close(nq.done)

		nq.Lock()
		defer nq.Unlock()

		close(nq.queue)
	}
}

// IsEmpty returns whether the queue is empty
func (nq *NotificationQueue) IsEmpty() bool {
	return len(nq.queue) == 0
}

// Next yields the next notification on the queue or stopped when processor has stopped
func (nq *NotificationQueue) Next(ctx context.Context) (*sql.ProjectionNotification, bool) {
	for {
		select {
		case <-nq.done:
			return nil, true
		case <-ctx.Done():
			return nil, true
		case notification := <-nq.queue:
			return notification, false
		}
	}
}

// Queue sends a notification to the queue
func (nq *NotificationQueue) Queue(ctx context.Context, notification *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to queue timeAwareNotification because the processor was stopped")
	}

	nq.metrics.QueueNotification(notification)

	nq.queueNotification(notification)
	return nil
}

// ReQueue will do nothing
func (nq *NotificationQueue) ReQueue(ctx context.Context, notification *sql.ProjectionNotification) error {
	return nil
}

func (nq *NotificationQueue) queueNotification(notification *sql.ProjectionNotification) {
	nq.Lock()
	defer nq.Unlock()

	nq.queue <- notification
}
