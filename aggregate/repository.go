package aggregate

import (
	"context"
	"errors"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/metadata"
)

const (
	// TypeKey is the metadata key to identify the aggregate type
	TypeKey = "_aggregate_type"
	// IDKey is the metadata key to identify the aggregate id
	IDKey = "_aggregate_id"
	// VersionKey is the metadata key to identify the aggregate version
	VersionKey = "_aggregate_version"
)

var (
	// ErrUnsupportedAggregateType occurs when the given aggregateType is not handled by the AggregateRepository
	ErrUnsupportedAggregateType = errors.New("goengine: the given AggregateRoot is of a unsupported type")
	// ErrUnexpectedMessageType occurs when the event store returns a message that is not an *aggregate.Changed
	ErrUnexpectedMessageType = errors.New("goengine: event store returned an unsupported message type")
	// ErrEmptyEventStream occurs when the event stream returned by the event store is empty
	ErrEmptyEventStream = errors.New("goengine: unsupported empty event stream")
)

type (
	// Repository a repository to save and load aggregate.Root's of a specific type
	Repository struct {
		aggregateType *Type
		eventStore    goengine.EventStore
		streamName    goengine.StreamName
	}
)

// NewRepository instantiates a new AggregateRepository
func NewRepository(
	eventStore goengine.EventStore,
	streamName goengine.StreamName,
	aggregateType *Type,
) (*Repository, error) {
	switch {
	case eventStore == nil:
		return nil, goengine.InvalidArgumentError("eventStore")
	case streamName == "":
		return nil, goengine.InvalidArgumentError("streamName")
	case aggregateType == nil:
		return nil, goengine.InvalidArgumentError("aggregateType")
	}

	repository := &Repository{
		eventStore:    eventStore,
		aggregateType: aggregateType,
		streamName:    streamName,
	}

	return repository, nil
}

// SaveAggregateRoot stores the state changes of the aggregate.Root
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

	streamEvents := make([]goengine.Message, len(domainEvents))
	for i, domainEvent := range domainEvents {
		streamEvents[i] = r.enrichMetadata(domainEvent, aggregateID)
	}

	return r.eventStore.AppendTo(ctx, r.streamName, streamEvents)
}

// GetAggregateRoot returns nil if no stream events can be found for aggregate id otherwise the reconstituted aggregate root
func (r *Repository) GetAggregateRoot(ctx context.Context, aggregateID ID) (Root, error) {
	matcher := metadata.NewMatcher()
	matcher = metadata.WithConstraint(matcher, TypeKey, metadata.Equals, r.aggregateType.String())
	matcher = metadata.WithConstraint(matcher, IDKey, metadata.Equals, aggregateID)

	streamEvents, err := r.eventStore.Load(ctx, r.streamName, 1, nil, matcher)
	if err != nil {
		return nil, err
	}
	defer streamEvents.Close()

	var changedStream []*Changed
	for streamEvents.Next() {
		msg, _, err := streamEvents.Message()
		if err != nil {
			return nil, err
		}

		changedEvent, ok := msg.(*Changed)
		if !ok {
			return nil, ErrUnexpectedMessageType
		}

		changedStream = append(changedStream, changedEvent)
	}

	if err := streamEvents.Err(); err != nil {
		return nil, err
	}

	if len(changedStream) == 0 {
		return nil, ErrEmptyEventStream
	}

	root := r.aggregateType.CreateInstance()
	root.replay(root, changedStream)

	return root, nil
}

// enrichEventMetadata add's aggregate_id and aggregate_type as metadata to domainEvent
func (r *Repository) enrichMetadata(aggregateEvent *Changed, aggregateID ID) *Changed {
	domainEvent := aggregateEvent.WithMetadata(IDKey, aggregateID)
	domainEvent = domainEvent.WithMetadata(TypeKey, r.aggregateType.String())
	domainEvent = domainEvent.WithMetadata(VersionKey, aggregateEvent.Version())

	return domainEvent.(*Changed)
}
