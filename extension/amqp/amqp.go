package amqp

import (
	"github.com/streadway/amqp"
)

// setup returns a connection and channel to be used for consumption with the Queue setup
func setup(url, queue string) (*amqp.Connection, *amqp.Channel, error) {
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
