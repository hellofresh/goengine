package amqp

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/mailru/easyjson"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/driver/sql"
)

var _ sql.ProjectionTrigger = (&NotificationPublisher{}).Publish

// NotificationPublisher is responsible for publishing a notification to queue
type NotificationPublisher struct {
	amqpDSN              string
	queue                string
	minReconnectInterval time.Duration
	maxReconnectInterval time.Duration
	logger               goengine.Logger

	connection io.Closer
	channel    NotificationChannel

	mux sync.Mutex
}

// NewNotificationPublisher returns an instance of NotificationPublisher
func NewNotificationPublisher(
	amqpDSN,
	queue string,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
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
		amqpDSN:              amqpDSN,
		queue:                queue,
		minReconnectInterval: minReconnectInterval,
		maxReconnectInterval: maxReconnectInterval,
		logger:               logger,
		connection:           connection,
		channel:              channel,
	}, nil
}

// Publish sends a ProjectionNotification to Queue
func (p *NotificationPublisher) Publish(_ context.Context, notification *sql.ProjectionNotification) error {
	reconnectInterval := p.minReconnectInterval
	// Ignore nil notifications since this is not supported
	// Skipping as we may receive a nil notification from dispatcher for the first time
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

			time.Sleep(reconnectInterval)
			reconnectInterval *= 2
			if reconnectInterval > p.maxReconnectInterval {
				reconnectInterval = p.maxReconnectInterval
			}

			p.connection = nil
			p.channel = nil
			continue
		}

		return err
	}
}
