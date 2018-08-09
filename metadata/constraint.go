package metadata

// Constraint is a struct representing a constraint that should be applied
// to the metadata of for example a Message
type Constraint struct {
	field    string
	operator Operator
	value    interface{}
}

// Field returns the metadata string field
func (c Constraint) Field() string {
	return c.field
}

// Operator returns the operator
func (c Constraint) Operator() Operator {
	return c.operator
}

// Value returns the scalar value
func (c Constraint) Value() interface{} {
	return c.value
}
