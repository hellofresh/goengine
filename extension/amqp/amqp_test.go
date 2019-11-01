package amqp_test

import "github.com/streadway/amqp"

type mockConnection struct {
}

type mockChannel struct {
}

func (cn mockConnection) Close() error {
	return nil
}

func (ch mockChannel) Publish(
	exchange string,
	queue string,
	mandatory bool,
	immediate bool,
	msg amqp.Publishing,
) error {
	return nil
}
