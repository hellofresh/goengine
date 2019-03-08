package aggregate

import (
	"sync"

	"github.com/hellofresh/goengine"
)

var (
	// Ensure BaseRoot implements the eventProducer interface
	_ eventProducer = &BaseRoot{}
	// Ensure BaseRoot implements the eventSourced interface
	_ eventSourced = &BaseRoot{}
)

// BaseRoot is the base struct to be embedded for any aggregate root
type BaseRoot struct {
	sync.Mutex
	version        uint
	recordedEvents []*Changed
}

// AggregateVersion returns the version of the aggregate
func (b *BaseRoot) AggregateVersion() uint {
	b.Lock()
	defer b.Unlock()

	return b.version
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

func (b *BaseRoot) replay(aggregate EventApplier, streamEvents goengine.EventStream) error {
	b.Lock()
	defer b.Unlock()

	for streamEvents.Next() {
		msg, _, err := streamEvents.Message()
		if err != nil {
			return err
		}

		changedEvent, ok := msg.(*Changed)
		if !ok {
			return ErrUnexpectedMessageType
		}

		b.version = changedEvent.Version()
		aggregate.Apply(changedEvent)
	}

	if err := streamEvents.Err(); err != nil {
		return err
	}

	if b.version == 0 {
		return ErrEmptyEventStream
	}

	return nil
}
