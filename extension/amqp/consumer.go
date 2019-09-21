package amqp

import (
	"context"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
)

type notificationConsumer struct {
}

func NewNotificationConsumer(amqpDSN string, projector sql.ProjectionTrigger, logger goengine.Logger) (*notificationConsumer, error) {
	return nil, nil
}

func (consumer *notificationConsumer) Consume(ctx context.Context) error {
	return nil
}
