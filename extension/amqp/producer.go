package amqp

import (
	"context"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/mailru/easyjson"
	"github.com/streadway/amqp"
)

var _ sql.ProjectionTrigger = (&NotificationPublisher{}).Publish

type NotificationPublisher struct {
	amqpDSN string
	queue   string
	logger  goengine.Logger
}

func NewNotificationPublisher(amqpDSN, queue string, logger goengine.Logger) (*NotificationPublisher, error) {
	return &NotificationPublisher{
		amqpDSN: amqpDSN,
		queue:   queue,
		logger:  logger,
	}, nil
}

func (p *NotificationPublisher) Publish(ctx context.Context, notification *sql.ProjectionNotification) error {
	// Ignore nil notifications since this is not supported
	if notification == nil {
		return nil
	}

	msgBody, err := easyjson.Marshal(notification)
	if err != nil {
		return err
	}

	// TODO use a persistent connection/channel
	conn, ch, err := setup(p.amqpDSN, p.queue)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			p.logger.Error("failed to close amqp connection", func(entry goengine.LoggerEntry) {
				entry.Error(err)
			})
		}
	}()

	return ch.Publish("", p.queue, true, false, amqp.Publishing{
		Body: msgBody,
	})
}
