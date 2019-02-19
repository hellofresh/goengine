package inmemory

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoengine(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "In Memory Suite")
}
