package goengine_test

import (
	. "github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/inmemory"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("A Event Store", func() {
	var eventStream *EventStream
	var store EventStore
	var aggregateId string
	var streamName StreamName

	store = inmemory.NewEventStore()

	BeforeEach(func() {
		eventStream = nil
		aggregateId = "594fb936-d646-44b5-a152-84eb4f709f20"
		streamName = "test"
	})

	JustBeforeEach(func() {
		var events []*DomainMessage

		events = append(events, RecordNow(aggregateId, 0, NewSomethingHappened()))
		events = append(events, RecordNow(aggregateId, 1, NewSomethingHappened()))
		events = append(events, RecordNow(aggregateId, 2, NewSomethingHappened()))

		eventStream = NewEventStream(streamName, events)
	})

	Describe("when something happens", func() {
		It("should save an event", func() {
			err := store.Append(eventStream)
			Expect(err).To(BeNil())
		})

		It("should retrive the things that happened", func() {
			expectedEvents, err := store.GetEventsFor(streamName, aggregateId)

			Expect(err).To(BeNil())
			Expect(expectedEvents.Events).To(HaveLen(3))
		})

		It("should count the events that happened", func() {
			Expect(store.CountEventsFor(streamName, aggregateId)).Should(Equal(int64(3)))
		})

		It("should retrieve events for version bigger then 1", func() {
			expectedEvents, err := store.FromVersion(streamName, aggregateId, 1)

			Expect(err).To(BeNil())
			Expect(expectedEvents.Events).To(HaveLen(2))
		})
	})
})
