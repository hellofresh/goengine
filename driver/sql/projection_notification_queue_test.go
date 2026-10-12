//go:build unit

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationQueueDeliversNotifications(t *testing.T) {
	nq := newNotificationQueue(10, 0, NopMetrics)
	queueClose := nq.Open()
	defer queueClose()

	ctx := context.Background()

	// A queued nil notification is a valid trigger for out-of-sync projections
	require.NoError(t, nq.Queue(ctx, nil))
	notification, stopped := nq.Next(ctx)
	assert.False(t, stopped)
	assert.Nil(t, notification)

	// A queued notification is delivered to the caller
	queued := &ProjectionNotification{No: 1, AggregateID: "abc"}
	require.NoError(t, nq.Queue(ctx, queued))
	notification, stopped = nq.Next(ctx)
	assert.False(t, stopped)
	assert.Equal(t, queued, notification)
}

func TestNotificationQueueStopsAfterClose(t *testing.T) {
	nq := newNotificationQueue(10, 0, NopMetrics)
	queueClose := nq.Open()

	ctx := context.Background()

	// Mimic the shutdown performed by the processor: the done channel is closed
	// followed by the queue channel. A processor observing this state must stop
	// and never receive the zero value of the closed queue channel as a phantom
	// nil notification.
	queueClose()

	for i := 0; i < 100; i++ {
		notification, stopped := nq.Next(ctx)
		assert.True(t, stopped, "Next must signal that the processor should stop")
		assert.Nil(t, notification)
	}
}
