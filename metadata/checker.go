package metadata

import "reflect"

// Matches returns true if the constraints in the matcher are all satisfied by the metadata
func Matches(matcher Matcher, metadata Metadata) (bool, error) {
	var constraints []Constraint
	matcher.Iterate(func(c Constraint) {
		constraints = append(constraints, c)
	})

	for _, c := range constraints {
		valid, err := matchConstraint(c, metadata.Value(c.Field()))
		if err != nil || !valid {
			return false, err
		}
	}

	return true, nil
}

func matchConstraint(c Constraint, val interface{}) (bool, error) {
	cVal, err := asScalar(c.Value())
	if err != nil {
		return false, err
	}
	cValType := reflect.TypeOf(cVal)

	// Ensure the value's are of the same type
	valType := reflect.TypeOf(val)
	if valType != cValType {
		// The types do not match let's see if they can be converted
		if !valType.ConvertibleTo(cValType) {
			return false, ErrTypeMismatch
		}

		val = reflect.ValueOf(val).Convert(cValType).Interface()
	}

	// Execute the comparison
	return compareValue(val, c.Operator(), cVal)
}
