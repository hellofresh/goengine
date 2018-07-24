package eventsourcing

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEventsourcing(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Eventsourcing Suite")
}
