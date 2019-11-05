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

	inMemoryQueue := NewNotificationQueue(4, nil)

	for _, notification := range notifications {
		go func(n *sql.ProjectionNotification) {
			err := inMemoryQueue.Queue(context.Background(), n)
			assert.NoError(t, err)
		}(&notification)
	}

	var called int
	var m sync.Mutex
	trigger := func(ctx context.Context, notification *sql.ProjectionNotification) error {
		m.Lock()
		defer m.Unlock()
		called++
		return nil
	}

	err = broker.Execute(context.Background(), inMemoryQueue, trigger)
	assert.NoError(t, err)

	assert.Equal(t, 3, called)
}
