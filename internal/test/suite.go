package test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/suite"
)

// Suite is an extension of github.com/stretchr/testify/suite.Suite
type Suite struct {
	suite.Suite

	Logger     *logrus.Logger
	LoggerHook *test.Hook
}

// SetupTest set logrus output to use the current testing.T
func (s *Suite) SetupTest() {
	s.Logger = logrus.New()
	s.Logger.SetLevel(logrus.DebugLevel)
	s.Logger.SetOutput(NewLogWriter(s.T()))

	s.LoggerHook = test.NewLocal(s.Logger)
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

// AssertNoLogsWithLevelOrHigher check that there are now log entries witch or of the given level or higher
// For example `AssertNoLogsWithLevelOrHigher(logrus.ErrorLevel)` will assert that no log entries with level error, fatal or panic where recorded.
func (s *Suite) AssertNoLogsWithLevelOrHigher(lvl logrus.Level) {
	assert := s.Assert()
	for _, logEntry := range s.LoggerHook.AllEntries() {
		assert.False(
			logEntry.Level <= lvl,
			"No error level log was expected but got: %s",
			logEntry.Message,
		)
	}
}
