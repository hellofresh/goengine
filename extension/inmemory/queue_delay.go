package inmemory

import (
	"context"
	"errors"
	"github.com/hellofresh/goengine/driver/sql"
	"sync"
	"time"
)

// Ensure the NotificationDelayQueue implements sql.NotificationQueuer
var _ sql.NotificationQueuer = &NotificationDelayQueue{}

type (
	// timeAwareNotification is a representation of the data provided by database notify
	timeAwareNotification struct {
		*sql.ProjectionNotification
		ValidAfter time.Time `json:"valid_after"`
	}

	// NotificationDelayQueue implements a smart queue
	NotificationDelayQueue struct {
		sync.Mutex

		retryDelay  time.Duration
		metrics     sql.Metrics
		done        chan struct{}
		queue       chan timeAwareNotification
		queueBuffer int
	}
)

func NewNotificationDelayQueue(queueBuffer int, retryDelay time.Duration, metrics sql.Metrics) *NotificationDelayQueue {
	if retryDelay == 0 {
		retryDelay = time.Millisecond * 50
	}

	return &NotificationDelayQueue{
		retryDelay:  retryDelay,
		metrics:     metrics,
		queueBuffer: queueBuffer,
	}
}

// Open enables the queue for business
func (nq *NotificationDelayQueue) Open() func() {
	nq.done = make(chan struct{})
	nq.queue = make(chan timeAwareNotification, nq.queueBuffer)

	return func() {
		close(nq.done)

		nq.Lock()
		defer nq.Unlock()

		close(nq.queue)
	}
}

// IsEmpty returns whether the queue is empty
func (nq *NotificationDelayQueue) IsEmpty() bool {
	return len(nq.queue) == 0
}

// Next yields the next notification on the queue or stopped when processor has stopped
func (nq *NotificationDelayQueue) Next(ctx context.Context) (*sql.ProjectionNotification, bool) {
	for {
		select {
		case <-nq.done:
			return nil, true
		case <-ctx.Done():
			return nil, true
		case notification := <-nq.queue:
			if notification.ValidAfter.After(time.Now()) {
				nq.queueNotification(notification)
				continue
			}
			return notification.ProjectionNotification, false
		}
	}
}

// Queue sends a notification to the queue
func (nq *NotificationDelayQueue) Queue(ctx context.Context, notification *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to queue timeAwareNotification because the processor was stopped")
	}

	nq.metrics.QueueNotification(notification)

	nq.queueNotification(timeAwareNotification{
		ProjectionNotification: notification,
	})
	return nil
}

// ReQueue sends a notification to the queue after setting the ValidAfter property
func (nq *NotificationDelayQueue) ReQueue(ctx context.Context, notification *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to re-queue timeAwareNotification because the processor was stopped")
	}

	nq.metrics.QueueNotification(notification)

	nq.queueNotification(timeAwareNotification{
		ProjectionNotification: notification,
		ValidAfter:             time.Now().Add(nq.retryDelay),
	})
	return nil
}

func (nq *NotificationDelayQueue) queueNotification(notification timeAwareNotification) {
	nq.Lock()
	defer nq.Unlock()

	nq.queue <- notification
}
