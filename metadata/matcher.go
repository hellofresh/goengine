package metadata

const (
	// Equals is a mathematical symbol used to indicate equality.
	// It is also a Drama/Science fiction film from 2015.
	Equals Operator = "="
	// GreaterThan is a mathematical symbol that denotes an inequality between two values.
	// It is typically placed between the two values being compared and signals that the first number is greater than the second number.
	GreaterThan Operator = ">"
	// GreaterThanEquals is a mathematical symbol that denotes an inequality between two values.
	// It is typically placed between the two values being compared and signals that the first number is greater than or equal to the second number.
	GreaterThanEquals Operator = ">="
	// LowerThan is a mathematical symbol that denotes an inequality between two values.
	// It is typically placed between the two values being compared and signals that the first number is less than the second number.
	LowerThan Operator = "<"
	// LowerThanEquals is a mathematical symbol that denotes an inequality between two values.
	// It is typically placed between the two values being compared and signals that the first number is less than or equal to the second number.
	LowerThanEquals Operator = "<="
	// NotEquals is a mathematical symbol that denotes an inequality between two values.
	// It is typically placed between the two values being compared and signals that the first number is not equal the second number.
	NotEquals Operator = "!="
)

var (
	// Ensure emptyMatcher implements the Matcher interface
	_ Matcher = new(emptyMatcher)
	// Ensure constraintMatcher implements the Matcher interface
	_ Matcher = &constraintMatcher{}
)

type (
	// Operator represents an operation for a constraint
	Operator string

	// Matcher is a struct used to register constraints that should be applied
	// to the metadata of for example a Message
	Matcher interface {
		// Iterate calls the callback for each constraint
		Iterate(callback func(constraint Constraint))
	}

	emptyMatcher int

	constraintMatcher struct {
		Matcher

		constraint Constraint
	}
)

// NewMatcher return a new Matcher instance without any constraints
func NewMatcher() Matcher {
	return new(emptyMatcher)
}

// WithConstraint add a constraint to the matcher
func WithConstraint(parent Matcher, field string, operator Operator, value interface{}) Matcher {
	return &constraintMatcher{
		parent,
		Constraint{
			field:    field,
			operator: operator,
			value:    value,
		},
	}
}

func (*emptyMatcher) Iterate(callback func(constraint Constraint)) {
}

func (c *constraintMatcher) Iterate(callback func(constraint Constraint)) {
	if c.Matcher != nil {
		c.Matcher.Iterate(callback)
	}

	callback(c.constraint)
}
