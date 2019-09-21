package amqp

import (
	"context"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
)

var _ sql.ProjectionTrigger = (&notificationPublisher{}).Publish

type notificationPublisher struct {
}

func NewNotificationPublisher(amqpDSN string, logger goengine.Logger) (*notificationPublisher, error) {
	return nil, nil
}

func (publisher *notificationPublisher) Publish(ctx context.Context, notification *sql.ProjectionNotification) error {
	return nil
}
