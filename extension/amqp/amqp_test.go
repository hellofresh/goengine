// +build unit

package amqp_test

import (
	"testing"

	"github.com/hellofresh/goengine/v2"
	goengineAmqp "github.com/hellofresh/goengine/v2/extension/amqp"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type mockAcknowledger struct {
}

func (ack mockAcknowledger) Ack(tag uint64, multiple bool) error {
	return nil
}

func (ack mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	return nil
}

func (ack mockAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

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

func TestDirectQueueConsume(t *testing.T) {
	t.Run("Invalid arguments", func(t *testing.T) {
		_, err := goengineAmqp.DirectQueueConsume("http://localhost:5672/", "my-queue")
		assert.Equal(t, goengine.InvalidArgumentError("amqpDSN"), err)

		_, err = goengineAmqp.DirectQueueConsume("amqp://localhost:5672/", "")
		assert.Equal(t, goengine.InvalidArgumentError("queue"), err)
	})

	t.Run("Returns amqp.Consume", func(t *testing.T) {
		c, err := goengineAmqp.DirectQueueConsume("amqp://localhost:5672/", "my-queue")
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.IsType(t, (goengineAmqp.Consume)(nil), c)
	})
}
