package inmemory

import "github.com/hellofresh/goengine"

// EventBus provides an in memory implementation of the VersionedEventPublisher VersionedEventReceiver interfaces
type EventBus struct {
	publishedEventsChannel chan *goengine.DomainMessage
	startReceiving         bool
}

// NewEventBus ...
func NewEventBus() *EventBus {
	publishedEventsChannel := make(chan *goengine.DomainMessage, 0)
	return &EventBus{publishedEventsChannel, false}
}

// PublishEvents publishes events to the event bus
func (bus *EventBus) PublishEvents(events []*goengine.DomainMessage) error {
	if !bus.startReceiving {
		return nil
	}

	for _, event := range events {
		bus.publishedEventsChannel <- event
	}

	return nil
}

// ReceiveEvents starts a go routine that monitors incoming events and routes them to a receiver channel specified within the options
func (bus *EventBus) ReceiveEvents(options goengine.VersionedEventReceiverOptions) error {
	bus.startReceiving = true

	go func() {
		for {
			select {
			case ch := <-options.Close:
				ch <- nil
			case versionedEvent := <-bus.publishedEventsChannel:
				ackCh := make(chan bool)
				options.ReceiveEvent <- goengine.VersionedEventTransactedAccept{
					Event:                 versionedEvent,
					ProcessedSuccessfully: ackCh,
				}
				<-ackCh
			}
		}
	}()

	return nil
}
