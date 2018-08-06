package aggregate_test

import (
	"testing"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/stretchr/testify/assert"
)

func TestReconstituteChange(t *testing.T) {
	t.Parallel()

	t.Run("It reconstitutes a Change message", func(t *testing.T) {
		// Mock message data
		id := aggregate.GenerateID()
		messageID := messaging.GenerateUUID()
		payload := struct {
			order int
		}{order: 1}
		msgMeta := metadata.New()
		msgMeta = metadata.WithValue(msgMeta, "auth", "none")
		createdOn := aggregate.CurrentTime()
		version := uint(10)

		// Reconstitute Change message
		msg, err := aggregate.ReconstituteChange(id, messageID, payload, msgMeta, createdOn, version)

		// Check Reconstituted message
		asserts := assert.New(t)
		asserts.NoError(err, "No error should be returned")
		asserts.NotEmpty(msg, "A message should be returned")
		asserts.Implements((*messaging.Message)(nil), msg)

		asserts.Equal(id, msg.AggregateID(), "Aggregate ID should be equal")
		asserts.Equal(messageID, msg.UUID(), "Message UUID should be equal")
		asserts.Equal(payload, msg.Payload(), "Message payload should be equal")
		asserts.Equal(msgMeta, msg.Metadata(), "Metadata should be equal")
		asserts.Equal(createdOn, msg.CreatedAt(), "Message createdAt should be equal")
		asserts.Equal(version, msg.Version(), "Message version should be equal")
	})

	t.Run("Check required arguments", func(t *testing.T) {
		t.Parallel()

		// Mock message data
		id := aggregate.GenerateID()
		messageID := messaging.GenerateUUID()
		payload := struct {
			order int
		}{order: 1}
		msgMeta := metadata.New()
		msgMeta = metadata.WithValue(msgMeta, "auth", "none")
		createdOn := aggregate.CurrentTime()
		version := uint(10)

		t.Run("aggregateID is required", func(t *testing.T) {
			invalidID := aggregate.ID("")

			// Reconstitute Change message
			msg, err := aggregate.ReconstituteChange(
				invalidID,
				messageID,
				payload,
				msgMeta,
				createdOn,
				version,
			)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrMissingAggregateID, err, "Expected aggregate.ID error")
			asserts.Empty(msg, "No message should be returned")
		})

		t.Run("message UUID is required", func(t *testing.T) {
			invalidMessageID := messaging.UUID{}

			// Reconstitute Change message
			msg, err := aggregate.ReconstituteChange(
				id,
				invalidMessageID,
				payload,
				msgMeta,
				createdOn,
				version,
			)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrMissingChangeUUID, err, "Expected messaging.ID error")
			asserts.Empty(msg, "No message should be returned")
		})

		t.Run("message payload is required", func(t *testing.T) {
			// Reconstitute Change message
			msg, err := aggregate.ReconstituteChange(
				id,
				messageID,
				nil,
				msgMeta,
				createdOn,
				version,
			)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrInvalidChangePayload, err, "Expected invalid payload error")
			asserts.Empty(msg, "No message should be returned")
		})

		t.Run("message version is required", func(t *testing.T) {
			// Mock message data
			var invalidVersion uint

			// Reconstitute Change message
			msg, err := aggregate.ReconstituteChange(
				id,
				messageID,
				payload,
				msgMeta,
				createdOn,
				invalidVersion,
			)

			// Check error
			asserts := assert.New(t)
			asserts.Equal(aggregate.ErrInvalidChangeVersion, err, "Expected messaging version error")
			asserts.Empty(msg, "No message should be returned")
		})
	})
}
