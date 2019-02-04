// +build unit

package goengine_dev_test

import (
	"testing"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/stretchr/testify/assert"
)

func TestGenerateUUID(t *testing.T) {
	zeroID := goengine_dev.UUID([16]byte{})

	asserts := assert.New(t)

	firstID := goengine_dev.GenerateUUID()
	asserts.NotEmpty(firstID, "A goengine_dev.UUID should not be empty")
	asserts.NotEqual(zeroID, firstID, "A goengine_dev.UUID should not be zero")

	secondID := goengine_dev.GenerateUUID()
	asserts.NotEmpty(secondID, "A goengine_dev.UUID should not be empty")
	asserts.NotEqual(zeroID, secondID, "A goengine_dev.UUID should not be zero")

	asserts.NotEqual(firstID, secondID, "Expected GenerateUUID() to return a different ID")
}
