package sql

import (
	"context"
	"errors"
	"time"
)

// Ensure the NotificationQueue.Queue is a ProjectionTrigger
var _ ProjectionTrigger = (&NotificationQueue{}).Queue

// Ensure the NotificationQueue.ReQueue is a ProjectionTrigger
var _ ProjectionTrigger = (&NotificationQueue{}).ReQueue

type (
	// NotificationQueueInterface describes a smart queue for projection notifications
	NotificationQueueInterface interface {
		Channel() chan *ProjectionNotification
		Close()
		Empty() bool
		Open(chan struct{})
		PutBack(*ProjectionNotification)
		Queue(context.Context, *ProjectionNotification) error
		ReQueue(context.Context, *ProjectionNotification) error
	}

	// NotificationQueue implements a smart queue
	NotificationQueue struct {
		retryDelay  time.Duration
		metrics     Metrics
		done        chan struct{}
		queue       chan *ProjectionNotification
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

// Channel returns the queue channel
func (nq *NotificationQueue) Channel() chan *ProjectionNotification {
	return nq.queue
}

// Close closes the queue channel
func (nq *NotificationQueue) Close() {
	close(nq.queue)
}

// Empty returns whether the queue is empty
func (nq *NotificationQueue) Empty() bool {
	return len(nq.queue) == 0
}

// PutBack sends a notification to the queue channel without further ado
func (nq *NotificationQueue) PutBack(notification *ProjectionNotification) {
	nq.queue <- notification
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

	nq.queue <- notification
	return nil
}

// ReQueue sends a notification to the queue after setting the ValidAfter property
func (nq *NotificationQueue) ReQueue(ctx context.Context, notification *ProjectionNotification) error {
	notification.ValidAfter = time.Now().Add(nq.retryDelay)

	return nq.Queue(ctx, notification)
}

// Open enables the queue for business
func (nq *NotificationQueue) Open(done chan struct{}) {
	nq.done = done
	nq.queue = make(chan *ProjectionNotification, nq.queueBuffer)
}
