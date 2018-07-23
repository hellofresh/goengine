package eventsourcing

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AggregateType", func() {
	mockInitiator := func() AggregateRoot {
		return &mockAggregateRoot{}
	}

	When("a new AggregateType is created for a AggregateRoot", func() {
		aggregateType, err := NewAggregateType("mock", mockInitiator)

		It("should have created a AggregateType", func() {
			Expect(err).To(BeNil())
			Expect(aggregateType).ToNot(BeNil())
		})

		It("should have the correct name", func() {
			Expect(aggregateType.String()).To(Equal("mock"))
		})

		It("can create new AggregateRoot's", func() {
			newRoot := aggregateType.newInstance()
			Expect(newRoot).ToNot(BeNil())

			secondRoot := aggregateType.newInstance()
			Expect(newRoot).ToNot(BeIdenticalTo(secondRoot))
		})

		It("can check if a instance of a AggregateRoot is of this type", func() {
			isInstanceOf := aggregateType.IsImplementedBy(&mockAggregateRoot{})
			Expect(isInstanceOf).To(BeTrue())

			isInstanceOf = aggregateType.IsImplementedBy(&customer{})
			Expect(isInstanceOf).To(BeFalse())
		})
	})

	Context("Construction validation", func() {
		It("requires a name", func() {
			aggregateType, err := NewAggregateType("", mockInitiator)

			Expect(aggregateType).To(BeNil())
			Expect(err).To(BeIdenticalTo(ErrAggregateTypeRequiresName))
		})

		It("initiator should not return nil", func() {
			aggregateType, err := NewAggregateType("string", func() AggregateRoot {
				return nil
			})

			Expect(aggregateType).To(BeNil())
			Expect(err).To(BeIdenticalTo(ErrAggregateInitiatorMustReturnPointer))
		})

		It("initiator should not return a struct", func() {
			aggregateType, err := NewAggregateType("string", func() AggregateRoot {
				return mockBadAggregateRootNotPointer{}
			})

			Expect(aggregateType).To(BeNil())
			Expect(err).To(BeIdenticalTo(ErrAggregateInitiatorMustReturnPointer))
		})

		It("initiator should not return a type that is not a reference to a struct", func() {
			aggregateType, err := NewAggregateType("string", func() AggregateRoot {
				return &mockBadAggregateRootNotStruct{}
			})

			Expect(aggregateType).To(BeNil())
			Expect(err).To(BeIdenticalTo(ErrAggregateInitiatorMustReturnPointerToStruct))
		})
	})
})

type customer struct {
	BaseAggregateRoot

	customerID AggregateID
}

func (c *customer) aggregateID() AggregateID {
	return c.customerID
}

func (c *customer) Apply(message AggregateChanged) {
}

type mockBadAggregateRootNotPointer struct {
	BaseAggregateRoot

	id AggregateID
}

func (a mockBadAggregateRootNotPointer) aggregateID() AggregateID {
	return a.id
}

func (mockBadAggregateRootNotPointer) Apply(message AggregateChanged) {
}

func (mockBadAggregateRootNotPointer) recordThat(aggregate EventApplier, event AggregateChanged) {
}

func (mockBadAggregateRootNotPointer) popRecordedEvents() []AggregateChanged {
	return nil
}

func (mockBadAggregateRootNotPointer) replay(aggregate EventApplier, historyEvents []AggregateChanged) {
}

type mockBadAggregateRootNotStruct map[string]interface{}

func (a *mockBadAggregateRootNotStruct) aggregateID() AggregateID {
	return GenerateAggregateID()
}

func (*mockBadAggregateRootNotStruct) Apply(message AggregateChanged) {
}

func (*mockBadAggregateRootNotStruct) recordThat(aggregate EventApplier, event AggregateChanged) {
}

func (*mockBadAggregateRootNotStruct) popRecordedEvents() []AggregateChanged {
	return nil
}

func (*mockBadAggregateRootNotStruct) replay(aggregate EventApplier, historyEvents []AggregateChanged) {
}
