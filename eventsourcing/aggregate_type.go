package eventsourcing

import (
	"reflect"

	"github.com/pkg/errors"
)

var (
	// ErrAggregateTypeRequiresName occurs when no name is provided
	ErrAggregateTypeRequiresName = errors.New("aggregate name may not be empty")
	// ErrAggregateInitiatorMustReturnPointer occurs when a AggregateInitiator did not return a pointer
	ErrAggregateInitiatorMustReturnPointer = errors.New("the AggregateInitiator must return a pointer to the AggregateRoot")
	// ErrAggregateInitiatorMustReturnPointerToStruct occurs when a AggregateInitiator did not return a pointer to a Struct
	ErrAggregateInitiatorMustReturnPointerToStruct = errors.New("the AggregateInitiator must return a pointer to the AggregateRoot struct")
)

type (
	// AggregateInitiator creates a new empty instance of a AggregateRoot
	// this instance may then be used to reconstitute the state of the aggregate root
	AggregateInitiator func() AggregateRoot

	// AggregateType an aggregate type represent the type of a aggregate
	AggregateType struct {
		name           string
		initiator      AggregateInitiator
		reflectionType reflect.Type
	}
)

// NewAggregateType create a new aggregate type instance
func NewAggregateType(name string, initiator AggregateInitiator) (*AggregateType, error) {
	if name == "" {
		return nil, ErrAggregateTypeRequiresName
	}

	// In order to avoid strange behavior the AggregateInitiator must return a pointer to a AggregateRoot instance
	reflectionType := reflect.TypeOf(initiator())
	if reflectionType == nil || reflectionType.Kind() != reflect.Ptr {
		return nil, ErrAggregateInitiatorMustReturnPointer
	}

	// The aggregate root's pointer must refer to a struct
	reflectionType = reflectionType.Elem()
	if reflectionType.Kind() != reflect.Struct {
		return nil, ErrAggregateInitiatorMustReturnPointerToStruct
	}

	return &AggregateType{
		name:           name,
		initiator:      initiator,
		reflectionType: reflectionType,
	}, nil
}

// String returns the name of the aggregate type
func (t *AggregateType) String() string {
	return t.name
}

// IsImplementedBy returns whether the given aggregate root is a instance of this aggregateType
func (t *AggregateType) IsImplementedBy(root AggregateRoot) bool {
	rootType := reflect.TypeOf(root)
	if rootType.Kind() != reflect.Ptr {
		return false
	}

	rootType = rootType.Elem()
	if rootType.Kind() != reflect.Struct {
		return false
	}

	return t.reflectionType == rootType
}

// newInstance returns an new empty/zero instance of the aggregate root that the type represents
func (t *AggregateType) newInstance() AggregateRoot {
	return t.initiator()
}
