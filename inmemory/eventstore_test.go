package inmemory

import (
	"github.com/hellofresh/goengine"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("In Memory Event Store", func() {
	var events []*goengine.DomainMessage
	var inMemory goengine.EventStore = NewEventStore()
	var aggregateID string
	var streamName goengine.StreamName

	BeforeEach(func() {
		events = nil                                         // clear the slice before each execution
		aggregateID = "eca7741f-b4c2-4fec-bfc7-be438a794be9" //uuid.New()
		streamName = "test"
	})

	JustBeforeEach(func() {
		events = append(events, goengine.RecordNow(aggregateID, 0, NewSomethingHappened()))
		events = append(events, goengine.RecordNow(aggregateID, 1, NewSomethingHappened()))
		events = append(events, goengine.RecordNow(aggregateID, 2, NewSomethingHappened()))
		events = append(events, goengine.RecordNow(aggregateID, 3, NewSomethingHappened()))
	})

	Describe("when something happens", func() {
		It("should save an event", func() {
			err := inMemory.Append(goengine.NewEventStream(streamName, events))
			Expect(err).To(BeNil())
		})

		It("should retrieve the things that happened", func() {
			stream, err := inMemory.GetEventsFor(streamName, aggregateID)

			Expect(err).To(BeNil())
			Expect(stream.Events).To(HaveLen(4))
		})

		It("should count the events that happened", func() {
			Expect(inMemory.CountEventsFor(streamName, aggregateID)).Should(Equal(int64(4)))
		})

		It("should retrieve events for version bigger then 1", func() {
			stream, err := inMemory.FromVersion(streamName, aggregateID, 1)

			Expect(err).To(BeNil())
			Expect(stream.Events).To(HaveLen(3))
		})
	})
})
