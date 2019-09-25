package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/reflection"
	"github.com/streadway/amqp"
)

const (
	defaultHeartbeat = 10 * time.Second
	defaultLocale    = "en_US"
)

// RawVersionedEvent represents the event that goes into rabbitmq
type RawVersionedEvent struct {
	ID         string          `bson:"aggregate_id,omitempty"`
	Version    int             `bson:"version"`
	Type       string          `bson:"type"`
	Payload    json.RawMessage `bson:"payload"`
	RecordedOn time.Time       `bson:"recorded_on"`
}

// EventBus  ...
type EventBus struct {
	brokerDSN      string
	queue          string
	exchange       string
	connectionName string
}

// NewEventBus ...
func NewEventBus(brokerDSN, queue, exchange, connectionName string) *EventBus {
	return &EventBus{brokerDSN, queue, exchange, connectionName}
}

// PublishEvents will publish events
func (bus *EventBus) PublishEvents(events []*goengine.DomainMessage) error {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, err := amqp.DialConfig(bus.brokerDSN, amqp.Config{
		Heartbeat:  defaultHeartbeat,
		Locale:     defaultLocale,
		Properties: amqp.Table{"connection_name": bus.connectionName + ".events-publisher"},
	})
	if err != nil {
		return err
	}

	// This waits for a server acknowledgment which means the sockets will have
	// flushed all outbound publishings prior to returning.  It's important to
	// block on Close to not lose any publishings.
	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("exchange.declare: %v", err)
	}

	for _, event := range events {
		rabbitEvent, err := bus.toRawEvent(event)
		if nil != err {
			return err
		}

		encodedEvent, err := json.Marshal(rabbitEvent)
		if nil != err {
			return err
		}

		// Prepare this message to be persistent.  Your publishing requirements may
		// be different.
		msg := amqp.Publishing{
			DeliveryMode:    amqp.Persistent,
			Timestamp:       time.Now(),
			ContentEncoding: "UTF-8",
			ContentType:     "application/json",
			Body:            encodedEvent,
		}

		err = c.Publish(bus.exchange, "", true, false, msg)
		if err != nil {
			// Since publish is asynchronous this can happen if the network connection
			// is reset or if the server has run out of resources.
			return fmt.Errorf("basic.publish: %v", err)
		}
	}

	return nil
}

// ReceiveEvents will receive events
func (bus *EventBus) ReceiveEvents(options goengine.VersionedEventReceiverOptions) error {
	conn, c, events, err := bus.consumeEventsQueue(options.Exclusive)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case ch := <-options.Close:
				defer conn.Close()
				ch <- c.Cancel(bus.queue, false)
				return

			case message, more := <-events:
				if more {
					var raw RawVersionedEvent
					if err := json.Unmarshal(message.Body, &raw); err != nil {
						options.Error <- fmt.Errorf("json.Unmarshal received event: %v", err)
					} else {
						domainEvent, err := bus.fromRawEvent(options.TypeRegistry, &raw)
						if nil != err {
							goengine.Log("EventBus.Cannot find event type", map[string]interface{}{"type": raw.Type}, nil)
							options.Error <- errors.New("Cannot find event type " + raw.Type)
							if err := message.Ack(true); err != nil {
								goengine.Log("EventBus.Ack could not ack message", nil, err)
							}
						} else {
							ackCh := make(chan bool)

							goengine.Log("EventBus.Dispatching Message", nil, nil)
							options.ReceiveEvent <- goengine.VersionedEventTransactedAccept{Event: domainEvent, ProcessedSuccessfully: ackCh}
							result := <-ackCh
							if err := message.Ack(result); err != nil {
								goengine.Log("EventBus.Ack could not ack dispatched message", nil, err)
							}
						}
					}
				} else {
					goengine.Log("RabbitMQ: Could have been disconnected", nil, nil)
					for {
						retryError := exponential(func() error {
							connR, cR, eventsR, errR := bus.consumeEventsQueue(options.Exclusive)
							if errR == nil {
								conn, c, events, err = connR, cR, eventsR, errR
							}

							goengine.Log("Failed to reconnect to RabbitMQ", nil, err)
							return errR
						}, 5)

						if retryError == nil {
							break
						}
					}
				}
			}
		}
	}()

	return nil
}

// DeleteQueue will delete a queue
func (bus *EventBus) DeleteQueue(name string) error {
	conn, err := amqp.DialConfig(bus.brokerDSN, amqp.Config{
		Heartbeat:  defaultHeartbeat,
		Locale:     defaultLocale,
		Properties: amqp.Table{"connection_name": bus.connectionName + ".queue-deleter"},
	})
	if err != nil {
		return err
	}

	c, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel.open: %s", err)
	}

	_, err = c.QueueDelete(name, false, false, true)
	return err
}

func (bus *EventBus) consumeEventsQueue(exclusive bool) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery, error) {
	conn, err := amqp.DialConfig(bus.brokerDSN, amqp.Config{
		Heartbeat:  defaultHeartbeat,
		Locale:     defaultLocale,
		Properties: amqp.Table{"connection_name": bus.connectionName + ".events-consumer"},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	c, err := conn.Channel()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("channel.open: %s", err)
	}

	// We declare our topology on both the publisher and consumer to ensure they
	// are the same.  This is part of AMQP being a programmable messaging model.
	//
	// See the Channel.Consume example for the complimentary declare.
	err = c.ExchangeDeclare(bus.exchange, "fanout", true, false, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("exchange.declare: %v", err)
	}

	if _, err = c.QueueDeclare(bus.queue, true, false, false, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.declare: %v", err)
	}

	if err = c.QueueBind(bus.queue, bus.queue, bus.exchange, false, nil); err != nil {
		return nil, nil, nil, fmt.Errorf("queue.bind: %v", err)
	}

	events, err := c.Consume(bus.queue, bus.queue, false, exclusive, false, false, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("basic.consume: %v", err)
	}

	if err := c.Qos(1, 0, false); err != nil {
		return nil, nil, nil, fmt.Errorf("qos: %v", err)
	}

	return conn, c, events, nil
}

func (bus *EventBus) toRawEvent(event *goengine.DomainMessage) (*RawVersionedEvent, error) {
	serializedPayload, err := json.Marshal(event.Payload)
	if nil != err {
		return nil, err
	}

	typeName := reflection.TypeOf(event.Payload)
	return &RawVersionedEvent{
		ID:         event.ID,
		Version:    event.Version,
		Type:       typeName.String(),
		Payload:    serializedPayload,
		RecordedOn: event.RecordedOn,
	}, nil
}

func (bus *EventBus) fromRawEvent(typeRegistry goengine.TypeRegistry, rabbitEvent *RawVersionedEvent) (*goengine.DomainMessage, error) {
	event, err := typeRegistry.Get(rabbitEvent.Type)
	if nil != err {
		return nil, err
	}

	err = json.Unmarshal(rabbitEvent.Payload, event)
	if nil != err {
		return nil, err
	}

	return goengine.NewDomainMessage(
		rabbitEvent.ID,
		rabbitEvent.Version,
		event.(goengine.DomainEvent),
		rabbitEvent.RecordedOn,
	), nil
}
