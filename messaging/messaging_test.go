// +build unit

package messaging_test

import (
	"testing"

	"github.com/hellofresh/goengine/messaging"
	"github.com/stretchr/testify/assert"
)

func TestGenerateUUID(t *testing.T) {
	zeroID := messaging.UUID([16]byte{})

	asserts := assert.New(t)

	firstID := messaging.GenerateUUID()
	asserts.NotEmpty(firstID, "A messaging.UUID should not be empty")
	asserts.NotEqual(zeroID, firstID, "A messaging.UUID should not be zero")

	secondID := messaging.GenerateUUID()
	asserts.NotEmpty(secondID, "A messaging.UUID should not be empty")
	asserts.NotEqual(zeroID, secondID, "A messaging.UUID should not be zero")

	asserts.NotEqual(firstID, secondID, "Expected GenerateUUID() to return a different ID")
}
