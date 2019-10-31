// +build unit

package inmemory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/driver/sql"
)

func TestInMemoryQueue(t *testing.T) {
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

	inMemoryQueue := NewNotificationQueue(3, nil)

	closeQueue := inMemoryQueue.Open()

	for _, notification := range notifications {
		err := inMemoryQueue.Queue(context.Background(), &notification)
		assert.NoError(t, err)
	}
	assert.Len(t, inMemoryQueue.queue, 3)

	for !inMemoryQueue.IsEmpty() {
		n, b := inMemoryQueue.Next(context.Background())
		assert.NotNil(t, n)
		assert.False(t, b)
	}
	assert.Len(t, inMemoryQueue.queue, 0)

	closeQueue()
	err := inMemoryQueue.Queue(context.Background(), nil)
	assert.Error(t, err)
}
