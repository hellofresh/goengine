package sql

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartProcessor(t *testing.T) {
	testCases := []struct {
		title        string
		notification func() *ProjectionNotification
		mockHandler  func(*testing.T, *projectionNotificationProcessor, context.CancelFunc) ProcessHandler
	}{
		{
			"Handle nil notification",
			func() *ProjectionNotification {
				return nil
			},
			func(t *testing.T, processor *projectionNotificationProcessor, cancel context.CancelFunc) ProcessHandler {
				return func(ctx context.Context, notification *ProjectionNotification, queue ProjectionTrigger) error {
					defer cancel()

					expectedFuncName := runtime.FuncForPC(reflect.ValueOf(processor.Queue).Pointer()).Name()
					actualFuncName := runtime.FuncForPC(reflect.ValueOf(queue).Pointer()).Name()
					require.Equal(t, expectedFuncName, actualFuncName)
					return nil
				}
			},
		},
		{
			"Handle new notification",
			func() *ProjectionNotification {
				return &ProjectionNotification{
					No:          1,
					AggregateID: "abc",
				}
			},
			func(t *testing.T, processor *projectionNotificationProcessor, cancel context.CancelFunc) ProcessHandler {
				return func(ctx context.Context, notification *ProjectionNotification, queue ProjectionTrigger) error {
					defer cancel()

					expectedFuncName := runtime.FuncForPC(reflect.ValueOf(processor.ReQueue).Pointer()).Name()
					actualFuncName := runtime.FuncForPC(reflect.ValueOf(queue).Pointer()).Name()
					require.Equal(t, expectedFuncName, actualFuncName)
					return nil
				}
			},
		},
		{
			"Handle retried notification",
			func() *ProjectionNotification {
				now := time.Now().Add(time.Millisecond * 200)
				return &ProjectionNotification{
					No:          1,
					AggregateID: "abc",
					ValidAfter:  now,
				}
			},
			func(t *testing.T, processor *projectionNotificationProcessor, cancel context.CancelFunc) ProcessHandler {
				return func(ctx context.Context, notification *ProjectionNotification, queue ProjectionTrigger) error {
					defer cancel()

					expectedFuncName := runtime.FuncForPC(reflect.ValueOf(processor.ReQueue).Pointer()).Name()
					actualFuncName := runtime.FuncForPC(reflect.ValueOf(queue).Pointer()).Name()
					require.Equal(t, expectedFuncName, actualFuncName)

					require.True(t, notification.ValidAfter.Before(time.Now()))
					return nil
				}
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			processor, err := newBackgroundProcessor(1, 1, nil, nil, 0)
			require.Nil(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			processor.queue = make(chan *ProjectionNotification, processor.queueBuffer)

			err = processor.Queue(ctx, testCase.notification())
			require.Nil(t, err)
			processor.startProcessor(ctx, testCase.mockHandler(t, processor, cancel))
		})
	}
}
