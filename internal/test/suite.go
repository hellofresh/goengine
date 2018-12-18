package test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// Suite is an extension of github.com/stretchr/testify/suite.Suite
type Suite struct {
	suite.Suite
}

// Run runs f as a subtest of t called name.
func (s *Suite) Run(name string, f func()) bool {
	parentT := s.T()
	return parentT.Run(name, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(parentT)
		f()
	})
}
