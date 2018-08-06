package aggregate

import (
	"sync"
)

// BaseRoot is the base struct to be embedded for any aggregate root
type BaseRoot struct {
	sync.Mutex
	version        uint
	recordedEvents []*Changed
}

func (b *BaseRoot) recordThat(aggregate EventApplier, event *Changed) {
	b.Lock()
	defer b.Unlock()

	b.version++
	event = event.withVersion(b.version)

	b.recordedEvents = append(b.recordedEvents, event)

	aggregate.Apply(event)
}

func (b *BaseRoot) popRecordedEvents() []*Changed {
	b.Lock()
	defer b.Unlock()

	pendingEvents := b.recordedEvents

	b.recordedEvents = nil

	return pendingEvents
}

func (b *BaseRoot) replay(aggregate EventApplier, historyEvents []*Changed) {
	b.Lock()
	defer b.Unlock()

	for _, pastEvent := range historyEvents {
		b.version = pastEvent.Version()

		aggregate.Apply(pastEvent)
	}
}
