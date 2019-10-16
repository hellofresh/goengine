package amqp

import (
	"io"

	"github.com/hellofresh/goengine"
	"github.com/streadway/amqp"
)

// NotificationChannel represents a channel for notifications
type NotificationChannel interface {
	Publish(exchange, queue string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
}

// setup returns a connection and channel to be used for the Queue setup
func setup(url, queue string) (io.Closer, NotificationChannel, error) {

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}

// DirectQueueConsume returns a Consume func that will connect to the provided AMQP server and create a queue for direct message delivery
func DirectQueueConsume(amqpDSN, queue string) (Consume, error) {
	switch {
	case len(amqpDSN) == 0:
		return nil, goengine.InvalidArgumentError("amqpDSN")
	case len(queue) == 0:
		return nil, goengine.InvalidArgumentError("queue")
	}

	return func() (io.Closer, <-chan amqp.Delivery, error) {
		conn, ch, err := setup(amqpDSN, queue)
		if err != nil {
			return nil, nil, err
		}

		// Indicate we only want 1 message to be acknowledge at a time.
		if err := ch.Qos(1, 0, false); err != nil {
			return nil, nil, err
		}

		// Exclusive consumer
		deliveries, err := ch.Consume(queue, "", true, false, false, false, nil)

		return conn, deliveries, err
	}, nil
}
