package goengine

// VersionedEventPublisher is responsible for publishing events that have been saved to the event store\repository
type VersionedEventPublisher interface {
	PublishEvents([]*DomainMessage) error
}

// VersionedEventReceiver is responsible for receiving globally published events
type VersionedEventReceiver interface {
	ReceiveEvents(VersionedEventReceiverOptions) error
}

// VersionedEventTransactedAccept is the message routed from an event receiver to the event manager.
// Sometimes event receivers designed with reliable delivery require acknowledgements after a message has been received. The success channel here allows for such acknowledgements
type VersionedEventTransactedAccept struct {
	Event                 *DomainMessage
	ProcessedSuccessfully chan bool
}

// VersionedEventReceiverOptions is an initalization structure to communicate to and from an event receiver go routine
type VersionedEventReceiverOptions struct {
	TypeRegistry TypeRegistry
	Close        chan chan error
	Error        chan error
	ReceiveEvent chan VersionedEventTransactedAccept
	Exclusive    bool
}

// VersionedEventHandler is a function that takes a versioned event
type VersionedEventHandler func(*DomainMessage) error

// VersionedEventDispatcher is responsible for routing events from the event manager to call handlers responsible for processing received events
type VersionedEventDispatcher interface {
	DispatchEvent(*DomainMessage) error
	RegisterEventHandler(event interface{}, handler VersionedEventHandler)
	RegisterGlobalHandler(handler VersionedEventHandler)
}
