package sql_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine/driver/sql"
	mocks "github.com/hellofresh/goengine/mocks/driver/sql"
	"github.com/stretchr/testify/require"
)

func TestStartProcessor(t *testing.T) {
	testCases := []struct {
		title        string
		notification func() *sql.ProjectionNotification
		queueFunc    string
	}{
		{
			"Handle nil notification",
			func() *sql.ProjectionNotification {
				return nil
			},
			"Queue",
		},
		{
			"Handle new notification",
			func() *sql.ProjectionNotification {
				return &sql.ProjectionNotification{
					No:          1,
					AggregateID: "abc",
				}
			},
			"ReQueue",
		},
		{
			"Handle retried notification",
			func() *sql.ProjectionNotification {
				return &sql.ProjectionNotification{
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
			ctrl := gomock.NewController(t)
			queueBufferSize := 1
			queueProcessorsCount := 1
			ctx := context.Background()
			notification := testCase.notification()

			notificationQueue := mocks.NewNotificationQueuer(ctrl)
			expect := notificationQueue.EXPECT()

			switch testCase.queueFunc {
			case "Queue":
				expect.Queue(gomock.Eq(ctx), gomock.Eq(notification)).Times(1)
			case "ReQueue":
				expect.ReQueue(gomock.Eq(ctx), gomock.Eq(notification)).Times(1)
			}

			done := make(chan struct{})
			channel := make(chan *sql.ProjectionNotification, queueBufferSize)
			channel <- notification
			called := false

			expect.Open().DoAndReturn(func() chan struct{} {
				return done
			}).AnyTimes()

			expect.Next(gomock.Eq(ctx)).DoAndReturn(func(ctx context.Context) (*sql.ProjectionNotification, bool) {
				if called {
					return nil, true
				}
				called = true
				return notification, false
			}).AnyTimes()

			expect.Close().Do(func() {
				close(channel)
			})

			processor, err := sql.NewBackgroundProcessor(queueProcessorsCount, queueBufferSize, nil, nil, notificationQueue)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)

			handler := func(ctx context.Context, notification *sql.ProjectionNotification, queue sql.ProjectionTrigger) error {
				err := queue(ctx, notification)
				require.NoError(t, err)

				wg.Done()
				return nil
			}

			killer := processor.Start(ctx, handler)

			defer func() {
				wg.Wait()
				killer()
				ctrl.Finish()
			}()
		})
	}
}
