package mongodb

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoengine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MongoDB EventStore Suite")
}
