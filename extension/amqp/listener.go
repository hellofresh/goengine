package amqp

import (
	"context"
	"io"
	"time"

	"github.com/mailru/easyjson"
	"github.com/streadway/amqp"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/driver/sql"
)

// Ensure Listener implements sql.Listener
var _ sql.Listener = &Listener{}

type (
	// Consume returns a channel of amqp.Delivery's and a related closer or an error
	Consume func() (io.Closer, <-chan amqp.Delivery, error)

	// Listener consumes messages from an queue
	Listener struct {
		consume              Consume
		minReconnectInterval time.Duration
		maxReconnectInterval time.Duration
		logger               goengine.Logger
		waitFn               func(time.Duration)
	}
)

// NewListener returns a new Listener
func NewListener(
	consume Consume,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
	logger goengine.Logger,
) (*Listener, error) {
	switch {
	case consume == nil:
		return nil, goengine.InvalidArgumentError("consume")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	return &Listener{
		consume:              consume,
		minReconnectInterval: minReconnectInterval,
		maxReconnectInterval: maxReconnectInterval,
		logger:               logger,
		waitFn:               time.Sleep,
	}, nil
}

// WithWaitFn replaces the default function called to wait (time.Sleep)
func (l *Listener) WithWaitFn(fn func(time.Duration)) {
	l.waitFn = fn
}

// Listen receives messages from a queue, transforms them into a sql.ProjectionNotification and calls the trigger
func (l *Listener) Listen(ctx context.Context, trigger sql.ProjectionTrigger) error {
	var nextReconnect time.Time
	reconnectInterval := l.minReconnectInterval
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		conn, deliveries, err := l.consume()
		if err != nil {
			l.logger.Error("failed to start consuming amqp messages", func(entry goengine.LoggerEntry) {
				entry.Error(err)
				entry.String("reconnect_in", reconnectInterval.String())
			})

			l.waitFn(reconnectInterval)
			reconnectInterval *= 2
			if reconnectInterval > l.maxReconnectInterval {
				reconnectInterval = l.maxReconnectInterval
			}
			continue
		}
		reconnectInterval = l.minReconnectInterval
		nextReconnect = time.Now().Add(reconnectInterval)

		l.consumeMessages(ctx, conn, deliveries, trigger)

		select {
		case <-ctx.Done():
			return context.Canceled
		default:
			l.waitFn(time.Until(nextReconnect))
		}
	}
}

func (l *Listener) consumeMessages(ctx context.Context, conn io.Closer, deliveries <-chan amqp.Delivery, trigger sql.ProjectionTrigger) {
	defer func() {
		if conn == nil {
			return
		}

		if err := conn.Close(); err != nil {
			l.logger.Error("failed to close amqp connection", func(entry goengine.LoggerEntry) {
				entry.Error(err)
			})
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-deliveries:
			if !ok {
				return
			}

			notification := &sql.ProjectionNotification{}
			if err := easyjson.Unmarshal(msg.Body, notification); err != nil {
				l.logger.Error("failed to unmarshal delivery, dropping message", func(entry goengine.LoggerEntry) {
					entry.Error(err)
				})
				continue
			}

			if err := msg.Ack(false); err != nil {
				l.logger.Error("failed to acknowledge notification delivery", func(entry goengine.LoggerEntry) {
					entry.Error(err)
					entry.Int64("notification.no", notification.No)
					entry.String("notification.aggregate_id", notification.AggregateID)
				})
				continue
			}

			if err := trigger(ctx, notification); err != nil {
				l.logger.Error("failed to project notification", func(entry goengine.LoggerEntry) {
					entry.Error(err)
					entry.Int64("notification.no", notification.No)
					entry.String("notification.aggregate_id", notification.AggregateID)
				})
			}

		}
	}
}
