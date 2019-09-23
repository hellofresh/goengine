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

	connection *amqp.Connection
	channel    *amqp.Channel
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
		p.logger.Warn("unable to handle nil notification, skipping", nil)
		return nil
	}

	msgBody, err := easyjson.Marshal(notification)
	if err != nil {
		return err
	}

	for {
		if p.connection == nil {
			p.connection, p.channel, err = setup(p.amqpDSN, p.queue)
			if err != nil {
				return err
			}
		}

		err = p.channel.Publish("", p.queue, true, false, amqp.Publishing{
			Body: msgBody,
		})
		if err == amqp.ErrClosed || err == amqp.ErrFrame || err == amqp.ErrUnexpectedFrame {
			if err := p.connection.Close(); err != nil {
				p.logger.Error("failed to close amqp connection", func(entry goengine.LoggerEntry) {
					entry.Error(err)
				})
			}
			p.connection = nil
			p.channel = nil
			continue
		}

		return err
	}
}
