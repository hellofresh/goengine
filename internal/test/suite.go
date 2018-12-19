package test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

// Suite is an extension of github.com/stretchr/testify/suite.Suite
type Suite struct {
	suite.Suite

	Logger *logrus.Logger
}

// SetupTest set logrus output to use the current testing.T
func (s *Suite) SetupTest() {
	s.Logger = logrus.New()
	s.Logger.SetLevel(logrus.DebugLevel)
	s.Logger.SetOutput(NewLogWriter(s.T()))
}

// TearDownTest cleanup suite variables
func (s *Suite) TearDownTest() {
	s.Logger = nil
}

// SetT sets the current *testing.T context
func (s *Suite) SetT(t *testing.T) {
	s.Suite.SetT(t)

	if s.Logger != nil {
		s.Logger.SetOutput(NewLogWriter(t))
	}
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
