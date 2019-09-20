package inmemory

import (
	"context"
	"errors"
	"github.com/hellofresh/goengine/driver/sql"
	"time"
)

// Ensure the NotificationQueue.Queue is a ProjectionTrigger
var _ sql.ProjectionTrigger = (&NotificationQueue{}).Queue

// Ensure the NotificationQueue.ReQueue is a ProjectionTrigger
var _ sql.ProjectionTrigger = (&NotificationQueue{}).ReQueue

type (
	// notification is a representation of the data provided by database notify
	notification struct {
		*sql.ProjectionNotification
		ValidAfter time.Time `json:"valid_after"`
	}

	// NotificationQueue implements a smart queue
	NotificationQueue struct {
		retryDelay  time.Duration
		metrics     sql.Metrics
		done        chan struct{}
		queue       chan notification
		queueBuffer int
	}
)

func NewNotificationQueue(queueBuffer int, retryDelay time.Duration, metrics sql.Metrics) *NotificationQueue {
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
func (nq *NotificationQueue) Open() chan struct{} {
	nq.done = make(chan struct{})
	nq.queue = make(chan notification, nq.queueBuffer)

	return nq.done
}

// Close closes the queue channel
func (nq *NotificationQueue) Close() {
	close(nq.queue)
}

// Empty returns whether the queue is empty
func (nq *NotificationQueue) Empty() bool {
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
			if notification.ValidAfter.After(time.Now()) {
				nq.queue <- notification
				continue
			}
			return notification.ProjectionNotification, false
		}
	}
}

// Queue sends a notification to the queue
func (nq *NotificationQueue) Queue(ctx context.Context, data *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to queue notification because the processor was stopped")
	}

	nq.metrics.QueueNotification(data)

	nq.queue <- notification{
		ProjectionNotification: data,
	}
	return nil
}

// ReQueue sends a notification to the queue after setting the ValidAfter property
func (nq *NotificationQueue) ReQueue(ctx context.Context, data *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-nq.done:
		return errors.New("goengine: unable to re-queue notification because the processor was stopped")
	}

	nq.metrics.QueueNotification(data)

	nq.queue <- notification{
		ProjectionNotification: data,
		ValidAfter:             time.Now().Add(nq.retryDelay),
	}
	return nil
}
