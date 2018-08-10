package inmemory

import (
	"errors"
	"reflect"

	"github.com/hellofresh/goengine/metadata"
	"github.com/sirupsen/logrus"
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
	IncompatibleMatcherError []error

	// MetadataMatcher an in memory metadata matcher implementation
	MetadataMatcher struct {
		logger      logrus.FieldLogger
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
func NewMetadataMatcher(matcher metadata.Matcher, logger logrus.FieldLogger) (*MetadataMatcher, error) {
	var constraints []metadataConstraint
	var constraintErrors IncompatibleMatcherError
	matcher.Iterate(func(c metadata.Constraint) {
		cVal, err := asScalar(c.Value())
		if err != nil {
			constraintErrors = append(constraintErrors, err)
			return
		}

		if !isSupportedOperator(cVal, c.Operator()) {
			constraintErrors = append(constraintErrors, ErrUnsupportedOperator)
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
					WithField("field", c.field).
					WithError(err).
					Warningf("metadata constraint failed with error")
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
func (IncompatibleMatcherError) Error() string {
	return "incompatible metadata.Matcher"
}
