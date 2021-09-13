//go:build unit
// +build unit

package metadata_test

import (
	"testing"

	"github.com/hellofresh/goengine/v2/metadata"
	"github.com/stretchr/testify/assert"
)

type internalConstraint struct {
	field    string
	operator metadata.Operator
	value    interface{}
}

func TestNewMatcher(t *testing.T) {
	asserts := assert.New(t)

	m1 := metadata.NewMatcher()
	asserts.NotNil(m1)

	m2 := metadata.NewMatcher()
	asserts.NotNil(m2)

	asserts.False(m1 == m2, "New instances should not be identical")
}

func TestMatcherIterate(t *testing.T) {
	t.Run("no constraints", func(t *testing.T) {
		m := metadata.NewMatcher()
		m.Iterate(func(constraint metadata.Constraint) {
			t.Fail()
		})
	})

	testCases := []struct {
		title               string
		matcher             metadata.Matcher
		expectedConstraints []internalConstraint
	}{
		{
			"multiple constraints",
			metadata.NewMatcher(),
			[]internalConstraint{
				{
					"key",
					metadata.Equals,
					"value",
				},
				{
					"version",
					metadata.NotEquals,
					50,
				},
				{
					"version",
					metadata.GreaterThanEquals,
					1,
				},
				{
					"version",
					metadata.LowerThanEquals,
					100,
				},
			},
		},
		{
			"nil matcher constraints",
			nil,
			[]internalConstraint{
				{
					"key",
					metadata.Equals,
					"value",
				},
				{
					"version",
					metadata.NotEquals,
					50,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run("multiple constraints", func(t *testing.T) {
			m := testCase.matcher
			for _, c := range testCase.expectedConstraints {
				m = metadata.WithConstraint(m, c.field, c.operator, c.value)
			}

			var constraints []internalConstraint
			m.Iterate(func(constraint metadata.Constraint) {
				constraints = append(constraints, internalConstraint{
					constraint.Field(),
					constraint.Operator(),
					constraint.Value(),
				})
			})

			assert.Equal(t, testCase.expectedConstraints, constraints)
		})

	}
}
