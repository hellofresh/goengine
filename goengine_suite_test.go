package goengine

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGoengine(t *testing.T) {
	SetLogHandler(func(msg string, fields map[string]interface{}, err error) {})

	RegisterFailHandler(Fail)
	RunSpecs(t, "GO Engine Suite")
}