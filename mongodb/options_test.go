package mongodb

import (
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MongoDB Event Store Context Background", func() {
	Describe("when I use ContextBackground option", func() {
		es := NewEventStore(nil, nil, ContextBackground())

		It("should have BackgroundContextStrategy context strategy", func() {
			Expect(es.cs).Should(BeAssignableToTypeOf(NewBackgroundContextStrategy()))
		})
	})
})

var _ = Describe("MongoDB Event Store Context Timeout", func() {
	Describe("when I use ContextTimeout option", func() {
		es := NewEventStore(nil, nil, ContextTimeout())

		It("should have TimeoutContextStrategy context strategy with some predefined values", func() {
			Expect(es.cs).Should(BeAssignableToTypeOf(NewTimeoutContextStrategy()))

			csTimeout, _ := es.cs.(*TimeoutContextStrategy)
			Expect(csTimeout.append).Should(BeNumerically(">", 0))
			Expect(csTimeout.getEventsFor).Should(BeNumerically(">", 0))
			Expect(csTimeout.fromVersion).Should(BeNumerically(">", 0))
			Expect(csTimeout.countEventsFor).Should(BeNumerically(">", 0))
			Expect(csTimeout.createIndices).Should(BeNumerically(">", 0))
		})
	})

	Describe("when I use ContextTimeout option with creation options", func() {
		appendTimeout := rand.Uint64()
		getEventsFor := rand.Uint64()
		fromVersion := rand.Uint64()
		countEventsFor := rand.Uint64()
		createIndices := rand.Uint64()

		es := NewEventStore(nil, nil, ContextTimeout(
			NewAppendTimeout(time.Duration(appendTimeout)),
			NewGetEventsForTimeout(time.Duration(getEventsFor)),
			NewFromVersionTimeout(time.Duration(fromVersion)),
			NewCountEventsForTimeout(time.Duration(countEventsFor)),
			NewCreateIndicesTimeout(time.Duration(createIndices)),
		))

		It("should have all the timeouts set respectively", func() {
			Expect(es.cs).Should(BeAssignableToTypeOf(NewTimeoutContextStrategy()))

			csTimeout, _ := es.cs.(*TimeoutContextStrategy)
			Expect(csTimeout.append).Should(BeEquivalentTo(appendTimeout))
			Expect(csTimeout.getEventsFor).Should(BeEquivalentTo(getEventsFor))
			Expect(csTimeout.fromVersion).Should(BeEquivalentTo(fromVersion))
			Expect(csTimeout.countEventsFor).Should(BeEquivalentTo(countEventsFor))
			Expect(csTimeout.createIndices).Should(BeEquivalentTo(createIndices))
		})
	})
})
