package eventsourcing

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AggregateChanged", func() {
	var (
		event        AggregateChanged
		eventPayload = struct{}{}
	)

	BeforeEach(func() {
		event = NewAggregateChange(GenerateAggregateID(), eventPayload)
	})

	Context("When a new instance is created", func() {
		It("Should set the createdAt", func() {
			Expect(event.CreatedAt()).ToNot(BeZero())

			By("creating a new event with a mocked time")
			currentTime := time.Now()
			timeNow = func() time.Time { return currentTime }

			newEvent := NewAggregateChange(GenerateAggregateID(), eventPayload)
			Expect(newEvent.CreatedAt()).To(BeIdenticalTo(currentTime))
		})

		It("Should generate a unique event uuid", func() {
			secondEvent := NewAggregateChange(GenerateAggregateID(), eventPayload)

			Expect(secondEvent.UUID()).ToNot(Equal(event.UUID()))
		})
	})

	Context("When metadata", func() {
		Context("is added", func() {
			It("Should not change the original", func() {
				newEvent1 := event.WithAddedMetadata("user_id", 42)
				Expect(newEvent1.Metadata()).To(Not(Equal(event.Metadata())))

				newEvent2 := event.WithAddedMetadata("trace_id", "80ce64b2-6d34-4d8e-9fb5-22e864d2d9d4")
				Expect(newEvent2.Metadata()).ToNot(Equal(event.Metadata()))
				Expect(newEvent2.Metadata()).ToNot(Equal(newEvent1.Metadata()))
			})

			It("Should not allow complex values", func() {
				aStruct := &struct {
					Test    string
					private string
				}{Test: "1", private: "me"}

				Expect(func() { event.WithAddedMetadata("ptr", aStruct) }).Should(Panic())
			})
		})

		Context("is returned", func() {
			It("Changes to the result should not change the origional", func() {
				metadata := event.Metadata()
				metadata["test"] = 1

				Expect(event.Metadata()).ToNot(HaveKey("test"))
			})
		})
	})

	Context("When a version is set", func() {
		It("Should return the new version", func() {
			newEvent := event.withVersion(99)

			Expect(newEvent.Version()).To(BeIdenticalTo(99))
		})

		It("Should not change the original", func() {
			newEvent := event.withVersion(1)

			Expect(newEvent.Version()).ToNot(Equal(event.Version()))
		})
	})
})
