package eventstore_test

import (
	. "github.com/hellofresh/goengine/eventstore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("In Memory Event Store", func() {
	var events []DomainEvent
	var inMemory *InMemoryEventStore = NewInMemoryEventStore()
	var aggregateId string
	var streamName StreamName

	BeforeEach(func() {
		events = nil // clear the slice before each execution
		aggregateId = "594fb936-d646-44b5-a152-84eb4f709f20"
		streamName = "test"
	})

	JustBeforeEach(func() {
		events = append(events, NewSomethingHappened())
		events = append(events, NewSomethingHappened())
		events = append(events, NewSomethingHappened())
		events = append(events, NewSomethingHappened())
	})

	Describe("when something happens", func() {
		It("should save an event", func() {
			for version, event := range events {
				message := RecordNow(aggregateId, version, event)
				inMemory.Save(streamName, message)
			}
		})

		It("should retrive the things that happened", func() {
			expectedEvents := inMemory.GetEventsFor(streamName, aggregateId)

			Expect(expectedEvents).To(HaveLen(4))
		})

		It("should count the events that happened", func() {
			Expect(inMemory.CountEventsFor(streamName, aggregateId)).Should(Equal(4))
		})

		It("should retrieve events for version bigger then 1", func() {
			expectedEvents := inMemory.FromVersion(streamName, aggregateId, 1)

			Expect(expectedEvents).To(HaveLen(3))
		})
	})
})
