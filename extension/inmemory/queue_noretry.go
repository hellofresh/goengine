package inmemory

import (
	"context"
	"errors"
	"github.com/hellofresh/goengine/driver/sql"
	"sync"
)

// Ensure the NotificationNoRetryQueue implements sql.NotificationQueuer
var _ sql.NotificationQueuer = &NotificationNoRetryQueue{}

// NotificationNoRetryQueue implements a smart queue
type NotificationNoRetryQueue struct {
	sync.Mutex

	metrics     sql.Metrics
	done        chan struct{}
	queue       chan *sql.ProjectionNotification
	queueBuffer int
}

func NewNotificationNoRetryQueue(queueBuffer int, metrics sql.Metrics) *NotificationNoRetryQueue {
	return &NotificationNoRetryQueue{
		metrics:     metrics,
		queueBuffer: queueBuffer,
	}
}

// Open enables the queue for business
func (nq *NotificationNoRetryQueue) Open() func() {
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
func (nq *NotificationNoRetryQueue) IsEmpty() bool {
	return len(nq.queue) == 0
}

// Next yields the next notification on the queue or stopped when processor has stopped
func (nq *NotificationNoRetryQueue) Next(ctx context.Context) (*sql.ProjectionNotification, bool) {
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
func (nq *NotificationNoRetryQueue) Queue(ctx context.Context, notification *sql.ProjectionNotification) error {
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
func (nq *NotificationNoRetryQueue) ReQueue(ctx context.Context, notification *sql.ProjectionNotification) error {
	return nil
}

func (nq *NotificationNoRetryQueue) queueNotification(notification *sql.ProjectionNotification) {
	nq.Lock()
	defer nq.Unlock()

	nq.queue <- notification
}
