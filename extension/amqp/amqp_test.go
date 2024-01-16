//go:build unit

package amqp_test

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/v2"
	goengineAmqp "github.com/hellofresh/goengine/v2/extension/amqp"
)

type mockAcknowledger struct {
}

func (ack mockAcknowledger) Ack(uint64, bool) error {
	return nil
}

func (ack mockAcknowledger) Nack(uint64, bool, bool) error {
	return nil
}

func (ack mockAcknowledger) Reject(uint64, bool) error {
	return nil
}

type mockConnection struct {
}

type mockChannel struct {
}

func (cn mockConnection) Close() error {
	return nil
}

func (ch mockChannel) Publish(string, string, bool, bool, amqp.Publishing) error {
	return nil
}

func (ch mockChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return make(chan amqp.Delivery), nil
}
func (ch mockChannel) Qos(int, int, bool) error {
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
