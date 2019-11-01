// +build unit

package amqp_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/extension/amqp"
	goengineLogger "github.com/hellofresh/goengine/extension/logrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	libamqp "github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListener_Listen(t *testing.T) {
	t.Run("Listen, consume and stop", func(t *testing.T) {
		ensure := require.New(t)

		ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
		defer ctxCancel()

		delivery1 := libamqp.Delivery{
			Body: []byte(`{"no": 1, "aggregate_id": "8150276e-34fe-49d9-aeae-a35af0040a4f"}`),
		}
		delivery1.Acknowledger = mockAcknowledger{}

		delivery2 := libamqp.Delivery{
			Body: []byte(`{"no": 2, "aggregate_id": "8150276e-34fe-49d9-aeae-a35af0040a4f"}`),
		}
		delivery2.Acknowledger = mockAcknowledger{}

		consumeCalls := 0
		consume := func() (io.Closer, <-chan libamqp.Delivery, error) {
			consumeCalls++
			ch := make(chan libamqp.Delivery, 2)
			ch <- delivery1
			ch <- delivery2
			return nil, ch, nil
		}
		triggerCalls := 0
		trigger := func(ctx context.Context, notification *sql.ProjectionNotification) error {
			triggerCalls++
			switch triggerCalls {
			case 1:
				ensure.Equal(&sql.ProjectionNotification{No: 1, AggregateID: "8150276e-34fe-49d9-aeae-a35af0040a4f"}, notification)
			case 2:
				ensure.Equal(&sql.ProjectionNotification{No: 2, AggregateID: "8150276e-34fe-49d9-aeae-a35af0040a4f"}, notification)
				ctxCancel()
			default:
				ensure.Fail("Only 2 calls to trigger where expected")
			}
			return nil
		}
		logger, loggerHook := getLogger()

		listener, err := amqp.NewListener(consume, time.Millisecond, time.Millisecond, logger)
		ensure.NoError(err)

		err = listener.Listen(ctx, trigger)

		ensure.Equal(context.Canceled, err)
		ensure.Equal(1, consumeCalls)
		ensure.Equal(2, triggerCalls)
		ensure.Len(loggerHook.Entries, 0)
	})

	t.Run("Reconnect with exponential back-off", func(t *testing.T) {
		ensure := require.New(t)

		ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
		defer ctxCancel()

		var consumeCalls []time.Time
		consume := func() (io.Closer, <-chan libamqp.Delivery, error) {
			consumeCalls = append(consumeCalls, time.Now())
			if len(consumeCalls) == 5 {
				ctxCancel()
			}

			return nil, nil, fmt.Errorf("failure %d", len(consumeCalls))
		}

		logger, loggerHook := getLogger()

		listener, err := amqp.NewListener(consume, time.Millisecond, 6*time.Millisecond, logger)
		ensure.NoError(err)

		err = listener.Listen(ctx, func(ctx context.Context, notification *sql.ProjectionNotification) error {
			ensure.Fail("Trigger should ever be called")
			return nil
		})

		ensure.Equal(context.Canceled, err)

		reconnectIntervals := []time.Duration{time.Millisecond, time.Millisecond * 2, time.Millisecond * 4, time.Millisecond * 6, time.Millisecond * 6}
		ensure.Len(consumeCalls, len(reconnectIntervals))
		for i := 1; i < len(reconnectIntervals); i++ {
			expectedInterval := reconnectIntervals[i-1]
			interval := consumeCalls[i].Sub(consumeCalls[i-1])

			if expectedInterval > interval || interval > (expectedInterval+time.Millisecond*2) {
				assert.Fail(t, fmt.Sprintf("Invalid interval after consume %d (got %s expected between %s and %s)", i, interval, expectedInterval, (expectedInterval+time.Millisecond)))
			}
		}

		// Ensure we get log output
		logEntries := loggerHook.AllEntries()
		ensure.Len(logEntries, len(reconnectIntervals))
		for i, log := range logEntries {
			assert.Equal(t, log.Level, logrus.ErrorLevel)
			assert.Equal(t, log.Message, "failed to start consuming amqp messages")
			assert.Equal(t, fmt.Errorf("failure %d", i+1), log.Data["error"])
			assert.Equal(t, reconnectIntervals[i].String(), log.Data["reconnect_in"])
		}
	})

	t.Run("Listen, consume and reconnect", func(t *testing.T) {
		ensure := require.New(t)

		ctx, ctxCancel := context.WithTimeout(context.Background(), time.Second)
		defer ctxCancel()

		consumeCalls := 0
		delivery1 := libamqp.Delivery{
			Body: []byte(`{"no": 1, "aggregate_id": "8150276e-34fe-49d9-aeae-a35af0040a4f"}`),
		}
		delivery1.Acknowledger = mockAcknowledger{}

		delivery2 := libamqp.Delivery{
			Body: []byte(`{"no": 2, "aggregate_id": "8150276e-34fe-49d9-aeae-a35af0040a4f"}`),
		}
		delivery2.Acknowledger = mockAcknowledger{}
		consume := func() (io.Closer, <-chan libamqp.Delivery, error) {
			consumeCalls++
			ch := make(chan libamqp.Delivery, 2)
			ch <- delivery1
			ch <- delivery2
			close(ch)
			return nil, ch, nil
		}
		triggerCalls := 0
		trigger := func(ctx context.Context, notification *sql.ProjectionNotification) error {
			triggerCalls++
			switch triggerCalls {
			case 1, 3:
				ensure.Equal(&sql.ProjectionNotification{No: 1, AggregateID: "8150276e-34fe-49d9-aeae-a35af0040a4f"}, notification)
			case 2, 4:
				ensure.Equal(&sql.ProjectionNotification{No: 2, AggregateID: "8150276e-34fe-49d9-aeae-a35af0040a4f"}, notification)
			default:
				ensure.Fail("Only 2 calls to trigger where expected")
			}
			if triggerCalls == 4 {
				ctxCancel()
			}
			return nil
		}

		logger, loggerHook := getLogger()

		listener, err := amqp.NewListener(consume, time.Millisecond, time.Millisecond, logger)
		ensure.NoError(err)

		err = listener.Listen(ctx, trigger)

		ensure.Equal(context.Canceled, err)
		ensure.Equal(2, consumeCalls)
		ensure.Equal(4, triggerCalls)
		ensure.Len(loggerHook.Entries, 0)
	})
}

func getLogger() (goengine.Logger, *test.Hook) {
	logger, loggerHook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	return goengineLogger.Wrap(logger), loggerHook
}
