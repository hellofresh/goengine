package amqp

import (
	"context"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/mailru/easyjson"
	"github.com/streadway/amqp"
)

var _ sql.Listener = &NotificationConsumer{}

type NotificationConsumer struct {
	amqpDSN string
	queue   string
	logger  goengine.Logger
}

func NewNotificationConsumer(
	amqpDSN,
	queue string,
	logger goengine.Logger,
) (*NotificationConsumer, error) {
	switch {
	case len(amqpDSN) == 0:
		return nil, goengine.InvalidArgumentError("amqpDSN")
	case len(queue) == 0:
		return nil, goengine.InvalidArgumentError("queue")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	return &NotificationConsumer{
		amqpDSN: amqpDSN,
		queue:   queue,
		logger:  logger,
	}, nil
}

func (c *NotificationConsumer) Listen(ctx context.Context, projector sql.ProjectionTrigger) error {
	for {
		conn, deliveries, err := c.consume()
		if err != nil {
			return err
		}
		closeConn := func() {
			if err := conn.Close(); err != nil {
				c.logger.Error("failed to close amqp connection", func(entry goengine.LoggerEntry) {
					entry.Error(err)
				})
			}
		}

		for msg := range deliveries {
			select {
			case <-ctx.Done():
				closeConn()
				return context.Canceled
			default:
			}

			notification := &sql.ProjectionNotification{}
			if err := easyjson.Unmarshal(msg.Body, notification); err != nil {
				c.logger.Error("failed to unmarshal delivery, dropping message", func(entry goengine.LoggerEntry) {
					entry.Error(err)
				})
				continue
			}

			if err := projector(ctx, notification); err != nil {
				c.logger.Error("failed to project notification", func(entry goengine.LoggerEntry) {
					entry.Error(err)
					entry.Int64("notification.no", notification.No)
					entry.String("notification.aggregate_id", notification.AggregateID)
				})
			}
		}

		closeConn()
	}
}

func (c *NotificationConsumer) consume() (*amqp.Connection, <-chan amqp.Delivery, error) {
	conn, ch, err := setup(c.amqpDSN, c.queue)
	if err != nil {
		return nil, nil, err
	}

	// Indicate we only want 1 message to acknowledge at a time.
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, nil, err
	}

	// Exclusive consumer
	deliveries, err := ch.Consume(c.queue, "", true, false, false, false, nil)

	return conn, deliveries, err
}
