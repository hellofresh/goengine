// +build unit

package inmemory

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/driver/sql"
)

func TestBroker(t *testing.T) {
	t.Run("Start broker to handle queued notifications", func(t *testing.T) {
		broker, err := NewNotificationBroker(3, nil, nil)
		assert.NoError(t, err)

		notifications := []sql.ProjectionNotification{
			{
				No:          1,
				AggregateID: string(aggregate.GenerateID()),
			},
			{
				No:          2,
				AggregateID: string(aggregate.GenerateID()),
			},
			{
				No:          3,
				AggregateID: string(aggregate.GenerateID()),
			},
		}

		inMemoryQueue := NewNotificationQueue(2, nil)

		triggerCalled := 0
		var m sync.Mutex
		trigger := func(ctx context.Context, notification *sql.ProjectionNotification) error {
			m.Lock()
			defer m.Unlock()
			triggerCalled++
			return nil
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			brokerCloser := broker.Start(context.Background(), inMemoryQueue, trigger)

			for _, notification := range notifications {
				err := inMemoryQueue.Queue(context.Background(), &notification)
				assert.NoError(t, err)
			}
			brokerCloser()
			wg.Done()

		}()
		wg.Wait()
		assert.Equal(t, 3, triggerCalled)
	})

	t.Run("Execute broker to handle notifications", func(t *testing.T) {
		broker, err := NewNotificationBroker(3, nil, nil)
		assert.NoError(t, err)

		notifications := []sql.ProjectionNotification{
			{
				No:          1,
				AggregateID: string(aggregate.GenerateID()),
			},
			{
				No:          2,
				AggregateID: string(aggregate.GenerateID()),
			},
			{
				No:          3,
				AggregateID: string(aggregate.GenerateID()),
			},
		}

		inMemoryQueue := NewNotificationQueue(2, nil)

		triggerCalled := 0
		var m sync.Mutex
		trigger := func(ctx context.Context, notification *sql.ProjectionNotification) error {
			m.Lock()
			defer m.Unlock()
			triggerCalled++
			return nil
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			err = broker.Execute(context.Background(), inMemoryQueue, trigger, &notifications[0])
			assert.NoError(t, err)
			err = broker.Execute(context.Background(), inMemoryQueue, trigger, &notifications[1])
			assert.NoError(t, err)
			err = broker.Execute(context.Background(), inMemoryQueue, trigger, &notifications[2])
			assert.NoError(t, err)
			wg.Done()
		}()
		wg.Wait()
		assert.Equal(t, 3, triggerCalled)
	})
}
