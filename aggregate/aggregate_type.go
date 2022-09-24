package aggregate

import (
	"errors"
	"reflect"

	"github.com/hellofresh/goengine/v2"
)

// ErrInitiatorMustReturnRoot occurs when an aggregate.Initiator did not return a pointer
var ErrInitiatorMustReturnRoot = errors.New("goengine: the aggregate.Initiator must return a pointer to the aggregate.Root")

type (
	// Initiator creates a new empty instance of an aggregate.Root
	// this instance may then be used to reconstitute the state of the aggregate root
	Initiator func() Root

	// Type an aggregate type represent the type of aggregate
	Type struct {
		name           string
		initiator      Initiator
		reflectionType reflect.Type
	}
)

// NewType create a new aggregate type instance
func NewType(name string, initiator Initiator) (*Type, error) {
	if name == "" {
		return nil, goengine.InvalidArgumentError("name")
	}

	// In order to avoid strange behavior the Initiator must return a pointer to an aggregate.Root instance
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

// IsImplementedBy returns whether the given aggregate root is an instance of this aggregateType
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

// CreateInstance returns a new empty/zero instance of the aggregate root that the type represents
func (t *Type) CreateInstance() Root {
	return t.initiator()
}
