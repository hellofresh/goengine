package sql

import (
	"context"
	"runtime"
	"sync"

	"github.com/hellofresh/goengine"
	"github.com/pkg/errors"
)

type (
	// ProjectionNotificationProcessor provides a way to Trigger a notification using a set of background processes.
	ProjectionNotificationProcessor struct {
		queueProcessors int

		logger  goengine.Logger
		metrics Metrics

		notificationQueue NotificationQueuer
	}

	// ProcessHandler is a func used to trigger a notification but with the addition of providing a Trigger func so
	// the original notification can trigger other notifications
	ProcessHandler func(context.Context, *ProjectionNotification, ProjectionTrigger) error
)

// NewBackgroundProcessor create a new projectionNotificationProcessor
func NewBackgroundProcessor(
	queueProcessors,
	queueBuffer int,
	logger goengine.Logger,
	metrics Metrics,
	notificationQueue NotificationQueuer,
) (*ProjectionNotificationProcessor, error) {
	if queueProcessors <= 0 {
		return nil, errors.New("queueProcessors must be greater then zero")
	}
	if queueBuffer < 0 {
		return nil, errors.New("queueBuffer must be greater or equal to zero")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}
	if metrics == nil {
		metrics = NopMetrics
	}
	if notificationQueue == nil {
		notificationQueue = newNotificationQueue(queueBuffer, 0, metrics)
	}

	return &ProjectionNotificationProcessor{
		queueProcessors:   queueProcessors,
		logger:            logger,
		metrics:           metrics,
		notificationQueue: notificationQueue,
	}, nil
}

// Execute starts the background worker and wait for the notification to be executed
func (b *ProjectionNotificationProcessor) Execute(ctx context.Context, handler ProcessHandler, notification *ProjectionNotification) error {
	// Wrap the processNotification in order to know that the first trigger finished
	handler, handlerDone := b.wrapProcessHandlerForSingleRun(handler)

	// Start the background processes
	stopExecutor := b.Start(ctx, handler)
	defer stopExecutor()

	// Execute a run of the internal.
	if err := b.notificationQueue.Queue(ctx, nil); err != nil {
		return err
	}

	// Wait for the trigger to be called or the context to be cancelled
	select {
	case <-handlerDone:
		return nil
	case <-ctx.Done():
		return nil
	}
}

// Start starts the background processes that will call the ProcessHandler based on the notification queued by Exec
func (b *ProjectionNotificationProcessor) Start(ctx context.Context, handler ProcessHandler) func() {
	queueClose := b.notificationQueue.Open()

	var wg sync.WaitGroup
	wg.Add(b.queueProcessors)
	for i := 0; i < b.queueProcessors; i++ {
		go func() {
			defer wg.Done()
			b.startProcessor(ctx, handler)
		}()
	}

	// Yield the processor so the go routines can start
	runtime.Gosched()

	return func() {
		queueClose()
		wg.Wait()
	}
}

// Queue puts the notification on the queue to be processed
func (b *ProjectionNotificationProcessor) Queue(ctx context.Context, notification *ProjectionNotification) error {
	return b.notificationQueue.Queue(ctx, notification)
}

func (b *ProjectionNotificationProcessor) startProcessor(ctx context.Context, handler ProcessHandler) {
	for {
		notification, stopped := b.notificationQueue.Next(ctx)
		if stopped {
			return
		}

		var queueFunc ProjectionTrigger
		if notification == nil {
			queueFunc = b.notificationQueue.Queue
		} else {
			queueFunc = b.notificationQueue.ReQueue
		}

		// Execute the notification
		b.metrics.StartNotificationProcessing(notification)
		if err := handler(ctx, notification, queueFunc); err != nil {
			b.logger.Error("the ProcessHandler produced an error", func(e goengine.LoggerEntry) {
				e.Error(err)
				e.Any("notification", notification)
			})

			b.metrics.FinishNotificationProcessing(notification, false)

		} else {
			b.metrics.FinishNotificationProcessing(notification, true)
		}
	}
}

// wrapProcessHandlerForSingleRun returns a wrapped ProcessHandler with a done channel that is closed after the
// provided ProcessHandler it's first call and related messages are finished or when the context is done.
func (b *ProjectionNotificationProcessor) wrapProcessHandlerForSingleRun(handler ProcessHandler) (ProcessHandler, chan struct{}) {
	done := make(chan struct{})
	var doneOnce sync.Once

	var m sync.Mutex
	var triggers int32
	return func(ctx context.Context, notification *ProjectionNotification, trigger ProjectionTrigger) error {
		m.Lock()
		triggers++
		m.Unlock()

		defer func() {
			m.Lock()
			defer m.Unlock()

			triggers--
			if triggers != 0 {
				return
			}

			// Only close the done channel when the queue is empty or the context is closed
			select {
			case <-done:
			case <-ctx.Done():
				// Context is expired
				doneOnce.Do(func() {
					close(done)
				})
			default:
				// No more queued messages to close the run
				if b.notificationQueue.Empty() {
					doneOnce.Do(func() {
						close(done)
					})
				}
			}
		}()

		return handler(ctx, notification, trigger)
	}, done
}
