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

func (ch mockChannel) Consume(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	return make(chan amqp.Delivery), nil
}
func (ch mockChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}
