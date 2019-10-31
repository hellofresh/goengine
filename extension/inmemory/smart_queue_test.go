package inmemory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/driver/sql"
)

func TestSmartQueue(t *testing.T) {
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

	smartQueue := NewNotificationSmartQueue(3, 0, nil)

	closeQueue := smartQueue.Open()

	for _, notification := range notifications {
		err := smartQueue.Queue(context.Background(), &notification)
		assert.NoError(t, err)
	}

	assert.Len(t, smartQueue.queue, 3)
	n, _ := smartQueue.Next(context.Background())
	assert.NotNil(t, n)
	assert.Len(t, smartQueue.queue, 2)

	err := smartQueue.ReQueue(context.Background(), &notifications[0])
	assert.NoError(t, err)
	assert.Len(t, smartQueue.queue, 3)

	for !smartQueue.IsEmpty() {
		n, _ := smartQueue.Next(context.Background())
		assert.NotNil(t, n)
	}
	assert.Len(t, smartQueue.queue, 0)

	closeQueue()
	err = smartQueue.Queue(context.Background(), nil)
	assert.Error(t, err)

	err = smartQueue.ReQueue(context.Background(), nil)
	assert.Error(t, err)
}
