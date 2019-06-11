package sql

import (
	"context"
	"runtime"
	"sync"

	"github.com/hellofresh/goengine"
	"github.com/pkg/errors"
)

// Ensure the projectionNotificationProcessor.Queue is a ProjectionTrigger
var _ ProjectionTrigger = (&projectionNotificationProcessor{}).Queue

type (
	// projectionNotificationProcessor provides a way to Trigger a notification using a set of background processes.
	projectionNotificationProcessor struct {
		done            chan struct{}
		queue           chan *ProjectionNotification
		queueProcessors int
		queueBuffer     int

		logger  goengine.Logger
		metrics goengine.Metrics
	}

	// ProcessHandler is a func used to trigger a notification but with the addition of providing a Trigger func so
	// the original notification can trigger other notifications
	ProcessHandler func(context.Context, *ProjectionNotification, ProjectionTrigger) error
)

// newBackgroundProcessor create a new projectionNotificationProcessor
func newBackgroundProcessor(queueProcessors, queueBuffer int, logger goengine.Logger, metrics goengine.Metrics) (*projectionNotificationProcessor, error) {
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
		metrics = goengine.NopMetrics
	}

	return &projectionNotificationProcessor{
		queueProcessors: queueProcessors,
		queueBuffer:     queueBuffer,
		logger:          logger,
		metrics:         metrics,
	}, nil
}

// Execute starts the background worker and wait for the notification to be executed
func (b *projectionNotificationProcessor) Execute(ctx context.Context, handler ProcessHandler, notification *ProjectionNotification) error {
	// Wrap the processNotification in order to know that the first trigger finished
	handler, handlerDone := b.wrapProcessHandlerForSingleRun(handler)

	// Start the background processes
	stopExecutor := b.Start(ctx, handler)
	defer stopExecutor()

	// Execute a run of the internal.
	if err := b.Queue(ctx, nil); err != nil {
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
func (b *projectionNotificationProcessor) Start(ctx context.Context, handler ProcessHandler) func() {
	b.done = make(chan struct{})
	b.queue = make(chan *ProjectionNotification, b.queueBuffer)

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
		close(b.done)
		wg.Wait()
		close(b.queue)
	}
}

// Queue puts the notification on the queue to be processed
func (b *projectionNotificationProcessor) Queue(ctx context.Context, notification *ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-b.done:
		return errors.New("goengine: unable to queue notification because the processor was stopped")
	}

	b.metrics.QueueNotification(notification)

	b.queue <- notification
	return nil
}

func (b *projectionNotificationProcessor) startProcessor(ctx context.Context, handler ProcessHandler) {
	for {
		select {
		case <-b.done:
			return
		case <-ctx.Done():
			return
		case notification := <-b.queue:
			// Execute the notification
			b.metrics.StartNotificationProcessing(notification)
			if err := handler(ctx, notification, b.Queue); err != nil {
				b.logger.Error("the ProcessHandler produced an error", func(e goengine.LoggerEntry) {
					e.Error(err)
					e.Any("notification", notification)
				})

				b.metrics.FinishNotificationProcessing(notification, false, false)

			} else {
				b.metrics.FinishNotificationProcessing(notification, true, false)
			}
		}
	}
}

// wrapProcessHandlerForSingleRun returns a wrapped ProcessHandler with a done channel that is closed after the
// provided ProcessHandler it's first call and related messages are finished or when the context is done.
func (b *projectionNotificationProcessor) wrapProcessHandlerForSingleRun(handler ProcessHandler) (ProcessHandler, chan struct{}) {
	done := make(chan struct{})

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
				close(done)
			default:
				// No more queued messages to close the run
				if len(b.queue) == 0 {
					close(done)
				}
			}
		}()

		return handler(ctx, notification, trigger)
	}, done
}
