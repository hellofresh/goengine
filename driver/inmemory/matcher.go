package inmemory

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/metadata"
)

var (
	// ErrTypeMismatch occurs when a metadata value cannot be converted to the constraints value type
	ErrTypeMismatch = errors.New("the values to compare are of a different type")
	// ErrUnsupportedOperator occurs when a constraints operation is not supported for a type
	ErrUnsupportedOperator = errors.New("the operator is not supported for this type")
)

type (
	// IncompatibleMatcherError is an error that constraints multiple errors.
	// This error is returned when a metadata.Matcher contains constraints that are
	// not supported by the inmemory.MetadataMatcher
	IncompatibleMatcherError []IncompatibleConstraintError

	// IncompatibleConstraintError is an error indicating that a constraint is incompatible.
	IncompatibleConstraintError struct {
		Parent     error
		Constraint metadata.Constraint
	}

	// MetadataMatcher an in memory metadata matcher implementation
	MetadataMatcher struct {
		logger      goengine.Logger
		constraints []metadataConstraint
	}

	// metadataConstraint an in memory metadata constraint
	metadataConstraint struct {
		field     string
		operator  metadata.Operator
		value     interface{}
		valueType reflect.Type
	}
)

// NewMetadataMatcher returns a new metadata matcher based of off the metadata.Matcher
func NewMetadataMatcher(matcher metadata.Matcher, logger goengine.Logger) (*MetadataMatcher, error) {
	var constraints []metadataConstraint
	var constraintErrors IncompatibleMatcherError
	matcher.Iterate(func(c metadata.Constraint) {
		cVal, err := asScalar(c.Value())
		if err != nil {
			constraintErrors = append(constraintErrors, IncompatibleConstraintError{err, c})
			return
		}

		if !isSupportedOperator(cVal, c.Operator()) {
			constraintErrors = append(constraintErrors, IncompatibleConstraintError{ErrUnsupportedOperator, c})
			return
		}

		constraints = append(constraints, metadataConstraint{
			c.Field(),
			c.Operator(),
			cVal,
			reflect.TypeOf(cVal),
		})
	})

	if len(constraintErrors) > 0 {
		return nil, constraintErrors
	}

	return &MetadataMatcher{
		constraints: constraints,
		logger:      logger,
	}, nil
}

// Matches returns true if the constraints in the matcher are all satisfied by the metadata
func (m *MetadataMatcher) Matches(metadata metadata.Metadata) bool {
	for _, c := range m.constraints {
		valid, err := c.Matches(metadata.Value(c.field))
		if err != nil {
			if m.logger != nil {
				m.logger.
					WithError(err).
					WithField("field", c.field).
					Warn("metadata constraint failed with error")
			}
			return false
		}
		if !valid {
			return false
		}
	}

	return true
}

// Matches returns true if the value satisfies the constraint
func (c *metadataConstraint) Matches(val interface{}) (bool, error) {
	// Ensure the value's are of the same type
	if valType := reflect.TypeOf(val); valType != c.valueType {
		// The types do not match let's see if they can be converted
		if !valType.ConvertibleTo(c.valueType) {
			return false, ErrTypeMismatch
		}

		val = reflect.ValueOf(val).Convert(c.valueType).Interface()
	}

	// Execute the comparison
	return c.compareValue(val)
}

// Error an error message
func (e IncompatibleMatcherError) Error() string {
	msg := bytes.NewBufferString("incompatible metadata.Matcher")
	for _, err := range e {
		msg.WriteRune('\n')
		msg.WriteString(err.Error())
	}

	return msg.String()
}

// Error an error message
func (e *IncompatibleConstraintError) Error() string {
	return fmt.Sprintf(
		"constaint %s %s %v is incompatible %s",
		e.Constraint.Field(),
		e.Constraint.Operator(),
		e.Constraint.Value(),
		e.Parent.Error(),
	)
}
