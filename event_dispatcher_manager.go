package goengine

import (
	log "github.com/Sirupsen/logrus"
)

// VersionedEventDispatchManager is responsible for coordinating receiving messages from event receivers and dispatching them to the event dispatcher.
type VersionedEventDispatchManager struct {
	versionedEventDispatcher *MapBasedVersionedEventDispatcher
	typeRegistry             TypeRegistry
	receiver                 VersionedEventReceiver
}

// NewVersionedEventDispatchManager is a constructor for the VersionedEventDispatchManager
func NewVersionedEventDispatchManager(receiver VersionedEventReceiver, registry TypeRegistry) *VersionedEventDispatchManager {
	return &VersionedEventDispatchManager{NewVersionedEventDispatcher(), registry, receiver}
}

// RegisterEventHandler allows a caller to register an event handler given an event of the specified type being received
func (m *VersionedEventDispatchManager) RegisterEventHandler(event interface{}, handler VersionedEventHandler) {
	m.typeRegistry.RegisterType(event)
	m.versionedEventDispatcher.RegisterEventHandler(event, handler)
}

// RegisterGlobalHandler allows a caller to register a wildcard event handler call on any event received
func (m *VersionedEventDispatchManager) RegisterGlobalHandler(handler VersionedEventHandler) {
	m.versionedEventDispatcher.RegisterGlobalHandler(handler)
}

// Listen starts a listen loop processing channels related to new incoming events, errors and stop listening requests
func (m *VersionedEventDispatchManager) Listen(stop <-chan bool, exclusive bool) error {
	// Create communication channels
	//
	// for closing the queue listener,
	closeChannel := make(chan chan error)
	// receiving errors from the listener thread (go routine)
	errorChannel := make(chan error)
	// and receiving events from the queue
	receiveEventChannel := make(chan VersionedEventTransactedAccept)

	// Start receiving events by passing these channels to the worker thread (go routine)
	options := VersionedEventReceiverOptions{m.typeRegistry, closeChannel, errorChannel, receiveEventChannel, exclusive}
	if err := m.receiver.ReceiveEvents(options); err != nil {
		return err
	}

	for {
		// Wait on multiple channels using the select control flow.
		select {
		// Version event received channel receives a result with a channel to respond to, signifying successful processing of the message.
		// This should eventually call an event handler. See cqrs.NewVersionedEventDispatcher()
		case event := <-receiveEventChannel:
			log.Debugf("EventDispatchManager.DispatchEvent: %s", event.Event)
			if err := m.versionedEventDispatcher.DispatchEvent(event.Event); err != nil {
				log.Println("Error dispatching event: ", err)
			}

			event.ProcessedSuccessfully <- true
			log.Debug("EventDispatchManager.DispatchSuccessful")
		case <-stop:
			log.Debug("EventDispatchManager.Stopping")
			closeSignal := make(chan error)
			closeChannel <- closeSignal
			defer log.Debug("EventDispatchManager.Stopped")
			return <-closeSignal
		// Receiving on this channel signifys an error has occured worker processor side
		case err := <-errorChannel:
			log.Debugf("EventDispatchManager.ErrorReceived: %s", err)
			return err
		}
	}
}
