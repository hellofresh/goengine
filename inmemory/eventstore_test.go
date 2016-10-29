package inmemory_test

import (
	. "github.com/hellofresh/goengine/eventstore"
	. "github.com/hellofresh/goengine/inmemory"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("In Memory Event Store", func() {
	var events []*DomainMessage
	var inMemory *InMemoryEventStore = NewEventStore()
	var aggregateId string
	var streamName StreamName

	BeforeEach(func() {
		events = nil                                         // clear the slice before each execution
		aggregateId = "eca7741f-b4c2-4fec-bfc7-be438a794be9" //uuid.New()
		streamName = "test"
	})

	JustBeforeEach(func() {
		events = append(events, RecordNow(aggregateId, 0, NewSomethingHappened()))
		events = append(events, RecordNow(aggregateId, 1, NewSomethingHappened()))
		events = append(events, RecordNow(aggregateId, 2, NewSomethingHappened()))
		events = append(events, RecordNow(aggregateId, 3, NewSomethingHappened()))
	})

	Describe("when something happens", func() {
		It("should save an event", func() {
			err := inMemory.Append(NewEventStream(streamName, events))
			Expect(err).To(BeNil())
		})

		It("should retrive the things that happened", func() {
			stream, err := inMemory.GetEventsFor(streamName, aggregateId)

			Expect(err).To(BeNil())
			Expect(stream.Events).To(HaveLen(4))
		})

		It("should count the events that happened", func() {
			Expect(inMemory.CountEventsFor(streamName, aggregateId)).Should(Equal(4))
		})

		It("should retrieve events for version bigger then 1", func() {
			stream, err := inMemory.FromVersion(streamName, aggregateId, 1)

			Expect(err).To(BeNil())
			Expect(stream.Events).To(HaveLen(3))
		})
	})
})
