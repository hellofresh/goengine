package eventsourcing

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AggregateTranslator", func() {
	translator := AggregateTranslator{}
	Context("extract information from a AggregateRoot", func() {
		aggregateID := GenerateAggregateID()
		mock := createMockAggregateRoot(aggregateID)

		It("can extract the aggregateID", func() {
			Expect(translator.ExtractAggregateID(mock)).To(BeIdenticalTo(aggregateID))
		})

		It("can extract the pending events", func() {
			events := translator.ExtractPendingStreamEvents(mock)
			Expect(events).To(HaveLen(1))
			Expect(events[0].Payload()).To(Equal(mockCreateEvent{MockID: aggregateID}))
		})
	})
})
