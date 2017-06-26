package goengine

// VersionedEventDispatchManager is responsible for coordinating receiving messages
// from event receivers and dispatching them to the event dispatcher.
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
		// Version event received channel receives a result with a channel to respond to,
		// signifying successful processing of the message.
		// This should eventually call an event handler. See NewVersionedEventDispatcher()
		case event := <-receiveEventChannel:
			fields := map[string]interface{}{"event": event.Event}
			Log("EventDispatchManager.DispatchEvent", fields, nil)
			if err := m.versionedEventDispatcher.DispatchEvent(event.Event); err != nil {
				Log("Error dispatching event", fields, err)
			}

			event.ProcessedSuccessfully <- true
			Log("EventDispatchManager.DispatchSuccessful", fields, nil)

		case <-stop:
			Log("EventDispatchManager.Stopping", nil, nil)
			closeSignal := make(chan error)
			closeChannel <- closeSignal
			Log("EventDispatchManager.Stopped", nil, nil)
			return <-closeSignal

		// Receiving on this channel signifys an error has occurred worker processor side
		case err := <-errorChannel:
			Log("EventDispatchManager.ErrorReceived", nil, err)
			return err
		}
	}
}
