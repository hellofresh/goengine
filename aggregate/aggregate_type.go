package aggregate

import (
	"errors"
	"reflect"
)

var (
	// ErrTypeNameRequired occurs when no name is provided
	ErrTypeNameRequired = errors.New("aggregate name may not be empty")
	// ErrInitiatorMustReturnRoot occurs when a aggregate.Initiator did not return a pointer
	ErrInitiatorMustReturnRoot = errors.New("the aggregate.Initiator must return a pointer to the aggregate.Root")
)

type (
	// Initiator creates a new empty instance of a aggregate.Root
	// this instance may then be used to reconstitute the state of the aggregate root
	Initiator func() Root

	// Type an aggregate type represent the type of a aggregate
	Type struct {
		name           string
		initiator      Initiator
		reflectionType reflect.Type
	}
)

// NewType create a new aggregate type instance
func NewType(name string, initiator Initiator) (*Type, error) {
	if name == "" {
		return nil, ErrTypeNameRequired
	}

	// In order to avoid strange behavior the Initiator must return a pointer to a aggregate.Root instance
	newRoot := initiator()
	reflectionType := reflect.TypeOf(newRoot)
	if reflectionType == nil || reflect.ValueOf(newRoot).IsNil() || reflectionType.Kind() != reflect.Ptr {
		return nil, ErrInitiatorMustReturnRoot
	}

	reflectionType = reflectionType.Elem()

	return &Type{
		name:           name,
		initiator:      initiator,
		reflectionType: reflectionType,
	}, nil
}

// String returns the name of the type
func (t *Type) String() string {
	return t.name
}

// IsImplementedBy returns whether the given aggregate root is a instance of this aggregateType
func (t *Type) IsImplementedBy(root interface{}) bool {
	if root == nil {
		return false
	}

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

// CreateInstance returns an new empty/zero instance of the aggregate root that the type represents
func (t *Type) CreateInstance() Root {
	return t.initiator()
}
