package inmemory

import (
	"github.com/hellofresh/goengine"
)

// InMemoryEventBus provides an inmemory implementation of the VersionedEventPublisher VersionedEventReceiver interfaces
type InMemoryEventBus struct {
	publishedEventsChannel chan *goengine.DomainMessage
	startReceiving         bool
}

// NewInMemoryEventBus constructor
func NewInMemoryEventBus() *InMemoryEventBus {
	publishedEventsChannel := make(chan *goengine.DomainMessage, 0)
	return &InMemoryEventBus{publishedEventsChannel, false}
}

// PublishEvents publishes events to the event bus
func (bus *InMemoryEventBus) PublishEvents(events []*goengine.DomainMessage) error {
	if !bus.startReceiving {
		return nil
	}

	for _, event := range events {
		bus.publishedEventsChannel <- event
	}

	return nil
}

// ReceiveEvents starts a go routine that monitors incoming events and routes them to a receiver channel specified within the options
func (bus *InMemoryEventBus) ReceiveEvents(options goengine.VersionedEventReceiverOptions) error {
	bus.startReceiving = true

	go func() {
		for {
			select {
			case ch := <-options.Close:
				ch <- nil
			case versionedEvent := <-bus.publishedEventsChannel:
				ackCh := make(chan bool)
				options.ReceiveEvent <- goengine.VersionedEventTransactedAccept{versionedEvent, ackCh}
				<-ackCh
			}
		}
	}()

	return nil
}
