package sql

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Ensure the NotificationQueue is a NotificationQueuer
var _ NotificationQueuer = &NotificationQueue{}

type (
	// NotificationQueuer describes a smart queue for projection notifications
	NotificationQueuer interface {
		Open() func()

		Empty() bool
		Next(context.Context) (*ProjectionNotification, bool)

		Queue(context.Context, *ProjectionNotification) error
		ReQueue(context.Context, *ProjectionNotification) error
	}

	// NotificationQueue implements a smart queue
	NotificationQueue struct {
		retryDelay  time.Duration
		metrics     Metrics
		done        chan struct{}
		queue       chan *ProjectionNotification
		queueLock   sync.Mutex
		queueBuffer int
	}
)

func newNotificationQueue(queueBuffer int, retryDelay time.Duration, metrics Metrics) *NotificationQueue {
	if retryDelay == 0 {
		retryDelay = time.Millisecond * 50
	}

	return &NotificationQueue{
		retryDelay:  retryDelay,
		metrics:     metrics,
		queueBuffer: queueBuffer,
	}
}

// Open enables the queue for business
func (nq *NotificationQueue) Open() func() {
	nq.queueLock.Lock()
	defer nq.queueLock.Unlock()

	nq.done = make(chan struct{})
	nq.queue = make(chan *ProjectionNotification, nq.queueBuffer)

	return func() {
		close(nq.done)

		nq.queueLock.Lock()
		defer nq.queueLock.Unlock()

		close(nq.queue)
	}
}

// Empty returns whether the queue is empty
func (nq *NotificationQueue) Empty() bool {
	return len(nq.queue) == 0
}

// Next yields the next notification on the queue or stopped when processor has stopped
func (nq *NotificationQueue) Next(ctx context.Context) (*ProjectionNotification, bool) {
	for {
		select {
		case <-nq.done:
			return nil, true
		case <-ctx.Done():
			return nil, true
		case notification := <-nq.queue:
			if notification != nil && notification.ValidAfter.After(time.Now()) {
				nq.queueNotification(notification)
				continue
			}
			return notification, false
		}
	}
}

// Queue sends a notification to the queue
func (nq *NotificationQueue) Queue(ctx context.Context, notification *ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to queue notification because the processor was stopped")
	}

	nq.metrics.QueueNotification(notification)

	nq.queueNotification(notification)

	return nil
}

// ReQueue sends a notification to the queue after setting the ValidAfter property
func (nq *NotificationQueue) ReQueue(ctx context.Context, notification *ProjectionNotification) error {
	notification.ValidAfter = time.Now().Add(nq.retryDelay)

	return nq.Queue(ctx, notification)
}

func (nq *NotificationQueue) queueNotification(notification *ProjectionNotification) {
	nq.queueLock.Lock()
	defer nq.queueLock.Unlock()

	nq.queue <- notification
}
