package mongodb

// Option is the options type to configure MongoDB EventStore implementation creation
type Option func(eventStore *EventStore)

// ContextBackground sets Context Strategy for EventStore to BackgroundContextStrategy
func ContextBackground() Option {
	return func(eventStore *EventStore) {
		eventStore.cs = NewBackgroundContextStrategy()
	}
}

// ContextTimeout sets Context Strategy for EventStore to TimeoutContextStrategy
func ContextTimeout(options ...TimeoutContextStrategyOption) Option {
	return func(eventStore *EventStore) {
		eventStore.cs = NewTimeoutContextStrategy(options...)
	}
}
