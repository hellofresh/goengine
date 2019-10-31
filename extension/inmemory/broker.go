package inmemory

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
)

type (
	NotificationWorker func()

	NotificationBroker struct {
		sync.Mutex
		queueProcessors int

		logger  goengine.Logger
		metrics sql.Metrics
	}
)

// NewNotificationBroker create a new NotificationBroker
func NewNotificationBroker(
	queueProcessors int,
	logger goengine.Logger,
	metrics sql.Metrics,
) (*NotificationBroker, error) {
	if queueProcessors <= 0 {
		return nil, errors.New("queueProcessors must be greater then zero")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}
	if metrics == nil {
		metrics = sql.NopMetrics
	}

	return &NotificationBroker{
		queueProcessors: queueProcessors,
		logger:          logger,
		metrics:         metrics,
	}, nil
}

// Execute starts the broker worker and wait for the notification to be executed
func (b *NotificationBroker) Execute(
	ctx context.Context,
	queue sql.NotificationQueuer,
	handler sql.ProjectionTrigger,
	notification *sql.ProjectionNotification,
) error {
	// Wrap the processNotification in order to know that the first trigger finished
	handler, handlerDone := b.wrapProcessHandlerForSingleRun(queue, handler)

	// Start the background processes
	stopExecutor := b.Start(ctx, queue, handler)
	defer stopExecutor()

	// Execute a run of the internal.
	if err := queue.Queue(ctx, nil); err != nil {
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

// Start starts the broker processes that will call the ProcessHandler based on the notification queued by Exec
func (b *NotificationBroker) Start(
	ctx context.Context,
	queue sql.NotificationQueuer,
	handler sql.ProjectionTrigger,
) func() {
	b.Lock()

	select {
	case <-ctx.Done():
		return func() { b.Unlock() }
	default:
	}

	queueClose := queue.Open()

	var wg sync.WaitGroup
	wg.Add(b.queueProcessors)
	for i := 0; i < b.queueProcessors; i++ {
		go func() {
			defer wg.Done()
			b.startProcessor(ctx, queue, handler)
		}()
	}

	// Yield the processor so the go routines can start
	runtime.Gosched()

	return func() {
		queueClose()
		wg.Wait()
		b.Unlock()
	}
}

func (b *NotificationBroker) startProcessor(
	ctx context.Context,
	queue sql.NotificationQueuer,
	handler sql.ProjectionTrigger,
) {
	for {
		notification, stopped := queue.Next(ctx)
		if stopped {
			return
		}

		// Execute the notification
		b.metrics.StartNotificationProcessing(notification)

		err := handler(ctx, notification)
		b.metrics.FinishNotificationProcessing(notification, err != nil)
	}
}

// wrapProcessHandlerForSingleRun returns a wrapped ProcessHandler with a done channel that is closed after the
// provided ProcessHandler it's first call and related messages are finished or when the context is done.
func (b *NotificationBroker) wrapProcessHandlerForSingleRun(
	queue sql.NotificationQueuer,
	handler sql.ProjectionTrigger,
) (sql.ProjectionTrigger, chan struct{}) {
	done := make(chan struct{})

	var m sync.Mutex
	var triggers int32
	return func(ctx context.Context, notification *sql.ProjectionNotification) error {
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
				close(done)
			default:
				// No more queued messages to close the run
				if queue.IsEmpty() {
					close(done)
				}
			}
		}()

		return handler(ctx, notification)
	}, done
}
