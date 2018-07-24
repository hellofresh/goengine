package eventsourcing

// AggregateTranslator make reconstitution and storage of aggregate roots possible by giving access to internal methods
type AggregateTranslator struct {
}

// ExtractPendingStreamEvents returns any pending events from the given aggregateRoot instance
func (*AggregateTranslator) ExtractPendingStreamEvents(aggregateRoot AggregateRoot) []AggregateChanged {
	return aggregateRoot.popRecordedEvents()
}

// ExtractAggregateID returns the aggregateID from the given aggregateRoot instance
func (*AggregateTranslator) ExtractAggregateID(aggregateRoot AggregateRoot) AggregateID {
	return aggregateRoot.aggregateID()
}

// ReplayStreamEvents replay the events onto the given aggregateRoot instance
func (*AggregateTranslator) ReplayStreamEvents(aggregateRoot AggregateRoot, events []AggregateChanged) {
	aggregateRoot.replay(aggregateRoot, events)
}

// ReconstituteAggregateFromHistory return an AggregateRoot with the given events applied
func (r *AggregateTranslator) ReconstituteAggregateFromHistory(aggregateType *AggregateType, historyEvents []AggregateChanged) AggregateRoot {
	aggregateRoot := aggregateType.newInstance()
	aggregateRoot.replay(aggregateRoot, historyEvents)

	return aggregateRoot
}
