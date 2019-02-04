package internal

import (
	"context"
	"runtime"
	"sync"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/log"
	"github.com/hellofresh/goengine/projector"
	"github.com/pkg/errors"
)

var (
	// ErrBackgroundWorkStopped occurs when queueing a notification when the BackgroundProcessor was stopped
	ErrBackgroundWorkStopped = errors.New("unable to queue notification because the processor was stopped")

	_ Trigger = (&BackgroundProcessor{}).Queue
)

type (
	// BackgroundProcessor provides a way to Trigger a notification using a set of background processes.
	BackgroundProcessor struct {
		done            chan struct{}
		queue           chan *projector.Notification
		queueProcessors int
		queueBuffer     int

		logger goengine_dev.Logger
	}

	// ProcessHandler is a func used to trigger a notification but with the addition of providing a Trigger func so
	// the original notification can trigger other notifications
	ProcessHandler func(context.Context, *projector.Notification, Trigger) error
)

// NewBackgroundProcessor create a new BackgroundProcessor
func NewBackgroundProcessor(queueProcessors, queueBuffer int, logger goengine_dev.Logger) (*BackgroundProcessor, error) {
	if queueProcessors <= 0 {
		return nil, errors.New("queueProcessors must be greater then zero")
	}
	if queueBuffer < 0 {
		return nil, errors.New("queueBuffer must be greater or equal to zero")
	}
	if logger == nil {
		logger = log.NilLogger
	}

	return &BackgroundProcessor{
		queueProcessors: queueProcessors,
		queueBuffer:     queueBuffer,
		logger:          logger,
	}, nil
}

// Execute starts the background worker and wait for the notification to be executed
func (b *BackgroundProcessor) Execute(ctx context.Context, handler ProcessHandler, notification *projector.Notification) error {
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
	b.queue = make(chan *projector.Notification, b.queueBuffer)

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
		close(b.queue)
		wg.Wait()
	}
}

// Queue puts the notification on the queue to be processed
func (b *BackgroundProcessor) Queue(ctx context.Context, notification *projector.Notification) error {
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
	for notification := range b.queue {
		select {
		default:
		case <-ctx.Done():
			// Context is expired
			return
		}

		// Execute the notification
		if err := handler(ctx, notification, b.Queue); err != nil {
			b.logger.
				WithError(err).
				WithField("notification", notification).
				Error("the ProcessHandler produced an error")
		}
	}
}

// wrapProcessHandlerForSingleRun returns a wrapped ProcessHandler with a done channel that is closed after the
// provided ProcessHandler it's first call and related messages are finished or when the context is done.
func (b *BackgroundProcessor) wrapProcessHandlerForSingleRun(handler ProcessHandler) (ProcessHandler, chan struct{}) {
	done := make(chan struct{})

	var m sync.Mutex
	var triggers int32
	return func(ctx context.Context, notification *projector.Notification, trigger Trigger) error {
		m.Lock()
		triggers++
		m.Unlock()

		defer func() {
			m.Lock()
			defer m.Unlock()

			triggers--
			if triggers != 0 || done == nil {
				return
			}

			// Only close the done channel when the queue is empty or the context is closed
			select {
			case <-ctx.Done():
				// Context is expired
				close(done)
				done = nil
			default:
				// No more queued messages to close the run
				if len(b.queue) == 0 {
					close(done)
					done = nil
				}
			}
		}()

		return handler(ctx, notification, trigger)
	}, done
}
