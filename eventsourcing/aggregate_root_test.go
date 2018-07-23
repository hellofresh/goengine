package eventsourcing

import (
	"github.com/hellofresh/goengine/messaging"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AggregateRoot", func() {
	Context("Create a new instance", func() {
		It("Should have record the created event", func() {
			id := GenerateAggregateID()
			aggregate := createMockAggregateRoot(id)

			Expect(aggregate.aggregateID()).To(BeIdenticalTo(id))
			Expect(aggregate.version).To(BeIdenticalTo(1))
			Expect(aggregate.recordedEvents).To(Equal(aggregate.AppliedEvents))

			By("extracting the recorded events no more events should be set")
			recordedEvents := aggregate.popRecordedEvents()

			Expect(recordedEvents).To(Equal(aggregate.AppliedEvents))
			Expect(recordedEvents[0].Payload()).To(Equal(mockCreateEvent{MockID: id}))

			Expect(aggregate.aggregateID()).To(BeIdenticalTo(id))
			Expect(aggregate.version).To(BeIdenticalTo(1))
			Expect(aggregate.recordedEvents).To(BeEmpty())
		})
	})

	Context("Creating a new instance by replaying", func() {
		It("Should be reconstituted in the original state", func() {
			id := GenerateAggregateID()
			historicalEvents := []AggregateChanged{
				&aggregateChanged{
					uuid:        messaging.GenerateUUID(),
					aggregateID: id,
					payload:     mockCreateEvent{MockID: id},
					createdAt:   timeNow(),
					version:     1,
					metadata:    map[string]interface{}{},
				},
			}

			aggregate := &mockAggregateRoot{}
			aggregate.replay(aggregate, historicalEvents)

			Expect(aggregate.aggregateID()).To(BeIdenticalTo(id))
			Expect(aggregate.version).To(BeIdenticalTo(1))
			Expect(aggregate.recordedEvents).To(BeEmpty())
			Expect(aggregate.AppliedEvents).To(Equal(historicalEvents))
		})
	})
})

type (
	mockCreateEvent struct {
		MockID AggregateID
	}

	mockAggregateRoot struct {
		BaseAggregateRoot

		AggregateID   AggregateID
		AppliedEvents []AggregateChanged
	}
)

func createMockAggregateRoot(id AggregateID) *mockAggregateRoot {
	root := &mockAggregateRoot{}

	event := mockCreateEvent{
		MockID: id,
	}

	RecordAggregateChange(root, event)

	return root
}

func (m *mockAggregateRoot) aggregateID() AggregateID {
	return m.AggregateID
}

func (m *mockAggregateRoot) Apply(message AggregateChanged) {
	m.AppliedEvents = append(m.AppliedEvents, message)

	switch message.Payload().(type) {
	case mockCreateEvent:
		event := message.Payload().(mockCreateEvent)
		m.AggregateID = event.MockID
	}
}
