package sql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type notificationQueueMock struct {
	mock.Mock
	done  chan struct{}
	queue chan *ProjectionNotification
}

func (m *notificationQueueMock) Start(done chan struct{}, queue chan *ProjectionNotification) {
	m.Called(done, queue)

	m.done = done
	m.queue = queue
}

func (m *notificationQueueMock) Queue(ctx context.Context, notification *ProjectionNotification) error {
	args := m.Called(ctx, notification)

	m.queue <- notification

	return args.Error(0)
}

func (m *notificationQueueMock) ReQueue(ctx context.Context, notification *ProjectionNotification) error {
	args := m.Called(ctx, notification)

	return args.Error(0)
}

func TestStartProcessor(t *testing.T) {
	testCases := []struct {
		title        string
		notification func() *ProjectionNotification
		queueMethod  string
	}{
		{
			"Handle nil notification",
			func() *ProjectionNotification {
				return nil
			},
			"Queue",
		},
		{
			"Handle new notification",
			func() *ProjectionNotification {
				return &ProjectionNotification{
					No:          1,
					AggregateID: "abc",
				}
			},
			"ReQueue",
		},
		{
			"Handle retried notification",
			func() *ProjectionNotification {
				return &ProjectionNotification{
					No:          1,
					AggregateID: "abc",
					ValidAfter:  time.Now().Add(time.Millisecond * 200),
				}
			},
			"ReQueue",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			bufferSize := 1
			queueProcessorsCount := 1
			retryDelay := time.Millisecond * 0

			queue := make(chan *ProjectionNotification, bufferSize)
			done := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			notification := testCase.notification()

			processor, err := newBackgroundProcessor(queueProcessorsCount, bufferSize, nil, nil, retryDelay)
			require.NoError(t, err)
			processor.done = done
			processor.queue = queue

			nqMock := &notificationQueueMock{}
			nqMock.On("Start", mock.Anything, queue).Return()
			nqMock.On(testCase.queueMethod, ctx, notification).Return(nil)
			nqMock.Start(processor.done, processor.queue)
			processor.notificationQueue = nqMock

			queue <- notification

			handler := func(ctx context.Context, notification *ProjectionNotification, queue ProjectionTrigger) error {
				defer cancel()

				err := queue(ctx, notification)
				require.NoError(t, err)

				nqMock.AssertExpectations(t)
				return nil
			}
			processor.startProcessor(ctx, handler)
		})
	}
}
