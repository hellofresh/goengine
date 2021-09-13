//go:build unit
// +build unit

package goengine_test

import (
	"testing"

	"github.com/hellofresh/goengine/v2"
	"github.com/stretchr/testify/assert"
)

func TestGenerateUUID(t *testing.T) {
	zeroID := goengine.UUID([16]byte{})

	asserts := assert.New(t)

	firstID := goengine.GenerateUUID()
	asserts.NotEmpty(firstID, "A goengine.UUID should not be empty")
	asserts.NotEqual(zeroID, firstID, "A goengine.UUID should not be zero")

	secondID := goengine.GenerateUUID()
	asserts.NotEmpty(secondID, "A goengine.UUID should not be empty")
	asserts.NotEqual(zeroID, secondID, "A goengine.UUID should not be zero")

	asserts.NotEqual(firstID, secondID, "Expected GenerateUUID() to return a different ID")
}
