package amqp

import (
	"context"
	"io"
	"sync"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql"
	"github.com/mailru/easyjson"
	"github.com/streadway/amqp"
)

var _ sql.ProjectionTrigger = (&NotificationPublisher{}).Publish

// NotificationPublisher is responsible of publishing a notification to queue
type NotificationPublisher struct {
	amqpDSN string
	queue   string
	logger  goengine.Logger

	connection io.Closer
	channel    NotificationChannel

	mux sync.Mutex
}

// NewNotificationPublisher returns an instance of NotificationPublisher
func NewNotificationPublisher(amqpDSN, queue string,
	logger goengine.Logger,
	connection io.Closer,
	channel NotificationChannel,
) (*NotificationPublisher, error) {

	if _, err := amqp.ParseURI(amqpDSN); err != nil {
		return nil, goengine.InvalidArgumentError("amqpDSN")
	}
	if len(queue) == 0 {
		return nil, goengine.InvalidArgumentError("queue")
	}
	return &NotificationPublisher{
		amqpDSN:    amqpDSN,
		queue:      queue,
		logger:     logger,
		connection: connection,
		channel:    channel,
	}, nil
}

// Publish sends a ProjectionNotification to Queue
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
		p.mux.Lock()
		if p.connection == nil {
			p.connection, p.channel, err = setup(p.amqpDSN, p.queue)
			if err != nil {
				return err
			}
		}
		p.mux.Unlock()

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
