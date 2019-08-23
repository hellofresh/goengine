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

			nqMock := mocks.NewMockNotificationQueueInterface(ctrl)
			ctx := context.Background()
			notification := testCase.notification()

			e := nqMock.EXPECT()
			queueCallCount := 0
			reQueueCallCount := 0
			switch testCase.queueFunc {
			case "Queue":
				queueCallCount++
			case "ReQueue":
				reQueueCallCount++
			}

			e.Queue(gomock.Eq(ctx), gomock.Eq(notification)).Times(queueCallCount)
			e.ReQueue(gomock.Eq(ctx), gomock.Eq(notification)).Times(reQueueCallCount)
			e.Open(gomock.Any()).AnyTimes()
			channel := make(chan *sql.ProjectionNotification, 1)
			channel <- notification
			e.Channel().Return(channel).AnyTimes()
			e.PutBack(gomock.Eq(notification)).Do(func(notification *sql.ProjectionNotification) {
				channel <- notification
			}).AnyTimes()
			e.Close()

			bufferSize := 1
			queueProcessorsCount := 1
			retryDelay := time.Millisecond * 0
			processor, err := sql.NewBackgroundProcessor(queueProcessorsCount, bufferSize, nil, nil, retryDelay, nqMock)
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
