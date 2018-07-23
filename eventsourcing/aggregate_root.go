package eventsourcing

import "sync"

// BaseAggregateRoot is the base struct to be embedded for any aggregate root
type BaseAggregateRoot struct {
	sync.Mutex
	version        int
	recordedEvents []AggregateChanged
}

func (b *BaseAggregateRoot) recordThat(aggregate EventApplier, event AggregateChanged) {
	b.Lock()
	defer b.Unlock()

	b.version++
	event = event.withVersion(b.version)

	b.recordedEvents = append(b.recordedEvents, event)

	aggregate.Apply(event)
}

func (b *BaseAggregateRoot) popRecordedEvents() []AggregateChanged {
	b.Lock()
	defer b.Unlock()

	pendingEvents := b.recordedEvents

	b.recordedEvents = nil

	return pendingEvents
}

func (b *BaseAggregateRoot) replay(aggregate EventApplier, historyEvents []AggregateChanged) {
	b.Lock()
	defer b.Unlock()

	for _, pastEvent := range historyEvents {
		b.version = pastEvent.Version()

		aggregate.Apply(pastEvent)
	}
}
