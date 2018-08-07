package aggregate

import (
	"context"
	"errors"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
)

const (
	// AggregateTypeKey is the metadata key to identify the aggregate type
	AggregateTypeKey = "_aggregate_type"
	// AggregateIDKey is the metadata key to identify the aggregate id
	AggregateIDKey = "_aggregate_id"
	// AggregateVersionKey is the metadata key to identify the aggregate version
	AggregateVersionKey = "_aggregate_version"
)

var (
	// ErrStreamNameRequired occurs when a empty stream name is provided
	ErrStreamNameRequired = errors.New("a StreamName may not be empty")
	// ErrEventStoreRequired occurs when a nil event store is provided
	ErrEventStoreRequired = errors.New("a EventStore may not be nil")
	// ErrTypeRequired occurs when a nil aggregate type is provided
	ErrTypeRequired = errors.New("a AggregateType may not be nil")
	// ErrUnsupportedAggregateType occurs when the given aggregateType is not handled by the AggregateRepository
	ErrUnsupportedAggregateType = errors.New("the given AggregateRoot is of a unsupported type")
	// ErrUnexpectedMessageType occurs when the event store returns a message that is not a *aggregate.Changed
	ErrUnexpectedMessageType = errors.New("event store returned a unsupported message type")
)

type (
	// Repository a repository to save and load aggregate.Root's of a specific type
	Repository struct {
		eventStore    eventstore.EventStore
		aggregateType *Type
		streamName    eventstore.StreamName
	}
)

// NewRepository instantiates a new AggregateRepository
func NewRepository(
	eventStore eventstore.EventStore,
	streamName eventstore.StreamName,
	aggregateType *Type,
) (*Repository, error) {
	if eventStore == nil {
		return nil, ErrEventStoreRequired
	}

	if streamName == "" {
		return nil, ErrStreamNameRequired
	}

	if aggregateType == nil {
		return nil, ErrTypeRequired
	}

	repository := &Repository{
		eventStore:    eventStore,
		aggregateType: aggregateType,
		streamName:    streamName,
	}

	return repository, nil
}

func (r *Repository) SaveAggregateRoot(ctx context.Context, aggregateRoot Root) error {
	if !r.aggregateType.IsImplementedBy(aggregateRoot) {
		return ErrUnsupportedAggregateType
	}

	domainEvents := aggregateRoot.popRecordedEvents()

	eventCount := len(domainEvents)
	if eventCount == 0 {
		return nil
	}

	aggregateID := aggregateRoot.AggregateID()

	streamEvents := make([]messaging.Message, len(domainEvents))
	for i, domainEvent := range domainEvents {
		streamEvents[i] = r.enrichMetadata(domainEvent, aggregateID)
	}

	return r.eventStore.AppendTo(ctx, r.streamName, streamEvents)
}

// GetAggregateRoot returns nil if no stream events can be found for aggregate id otherwise the reconstituted aggregate root
func (r *Repository) GetAggregateRoot(ctx context.Context, aggregateID ID) (Root, error) {
	matcher := metadata.NewMatcher()
	matcher = metadata.WithConstraint(matcher, AggregateTypeKey, metadata.Equals, r.aggregateType.String())
	matcher = metadata.WithConstraint(matcher, AggregateIDKey, metadata.Equals, aggregateID)

	streamEvents, err := r.eventStore.Load(ctx, r.streamName, 1, nil, matcher)
	if err != nil {
		return nil, err
	}

	changedStream := make([]*Changed, len(streamEvents))
	for i, streamEvent := range streamEvents {
		changedEvent, ok := streamEvent.(*Changed)
		if !ok {
			return nil, ErrUnexpectedMessageType
		}

		changedStream[i] = changedEvent
	}

	root := r.aggregateType.CreateInstance()
	root.replay(root, changedStream)

	return root, nil
}

// enrichEventMetadata add's aggregate_id and aggregate_type as metadata to domainEvent
func (r *Repository) enrichMetadata(aggregateEvent *Changed, aggregateID ID) *Changed {
	domainEvent := aggregateEvent.WithMetadata(AggregateIDKey, aggregateID)
	domainEvent = domainEvent.WithMetadata(AggregateTypeKey, r.aggregateType.String())
	domainEvent = domainEvent.WithMetadata(AggregateVersionKey, aggregateEvent.Version())

	return domainEvent.(*Changed)
}
