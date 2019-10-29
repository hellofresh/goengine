package amqp

import (
	"io"

	"github.com/streadway/amqp"
)

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

// NotificationChannel represents a channel for notifications
type NotificationChannel interface {
	Publish(exchange, queue string, mandatory, immediate bool, msg amqp.Publishing) error
}
