package internal

import (
	"context"
	"runtime"
	"sync"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/pkg/errors"
)

var (
	// ErrBackgroundWorkStopped occurs when queueing a notification when the BackgroundProcessor was stopped
	ErrBackgroundWorkStopped = errors.New("goengine: unable to queue notification because the processor was stopped")

	// Ensure the BackgroundProcessor.Queue is a ProjectionTrigger
	_ sql.ProjectionTrigger = (&BackgroundProcessor{}).Queue
)

type (
	// BackgroundProcessor provides a way to Trigger a notification using a set of background processes.
	BackgroundProcessor struct {
		done            chan struct{}
		queue           chan *sql.ProjectionNotification
		queueProcessors int
		queueBuffer     int

		logger goengine.Logger
	}

	// ProcessHandler is a func used to trigger a notification but with the addition of providing a Trigger func so
	// the original notification can trigger other notifications
	ProcessHandler func(context.Context, *sql.ProjectionNotification, sql.ProjectionTrigger) error
)

// NewBackgroundProcessor create a new BackgroundProcessor
func NewBackgroundProcessor(queueProcessors, queueBuffer int, logger goengine.Logger) (*BackgroundProcessor, error) {
	if queueProcessors <= 0 {
		return nil, errors.New("queueProcessors must be greater then zero")
	}
	if queueBuffer < 0 {
		return nil, errors.New("queueBuffer must be greater or equal to zero")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}

	return &BackgroundProcessor{
		queueProcessors: queueProcessors,
		queueBuffer:     queueBuffer,
		logger:          logger,
	}, nil
}

// Execute starts the background worker and wait for the notification to be executed
func (b *BackgroundProcessor) Execute(ctx context.Context, handler ProcessHandler, notification *sql.ProjectionNotification) error {
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
func (b *BackgroundProcessor) Start(ctx context.Context, handler ProcessHandler) func() {
	b.done = make(chan struct{})
	b.queue = make(chan *sql.ProjectionNotification, b.queueBuffer)

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
func (b *BackgroundProcessor) Queue(ctx context.Context, notification *sql.ProjectionNotification) error {
	select {
	default:
	case <-ctx.Done():
		return context.Canceled
	case <-b.done:
		return ErrBackgroundWorkStopped
	}

	b.queue <- notification
	return nil
}

func (b *BackgroundProcessor) startProcessor(ctx context.Context, handler ProcessHandler) {
	for {
		select {
		case <-b.done:
			return
		case <-ctx.Done():
			return
		case notification := <-b.queue:
			// Execute the notification
			if err := handler(ctx, notification, b.Queue); err != nil {
				b.logger.Error("the ProcessHandler produced an error", func(e goengine.LoggerEntry) {
					e.Error(err)
					e.Any("notification", notification)
				})
			}
		}
	}
}

// wrapProcessHandlerForSingleRun returns a wrapped ProcessHandler with a done channel that is closed after the
// provided ProcessHandler it's first call and related messages are finished or when the context is done.
func (b *BackgroundProcessor) wrapProcessHandlerForSingleRun(handler ProcessHandler) (ProcessHandler, chan struct{}) {
	done := make(chan struct{})

	var m sync.Mutex
	var triggers int32
	return func(ctx context.Context, notification *sql.ProjectionNotification, trigger sql.ProjectionTrigger) error {
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
