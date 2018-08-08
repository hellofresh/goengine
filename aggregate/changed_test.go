package aggregate_test

import (
	"testing"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/metadata"
	"github.com/stretchr/testify/assert"
)

func TestReconstituteChange(t *testing.T) {
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

		// Define the test cases
		errorCases := []struct {
			title         string
			aggregateID   aggregate.ID
			uuid          messaging.UUID
			payload       interface{}
			metadata      metadata.Metadata
			createdAt     time.Time
			version       uint
			expectedError error
		}{
			{
				title:         "aggregateID is required",
				expectedError: aggregate.ErrMissingAggregateID,
				aggregateID:   aggregate.ID(""),
				uuid:          messageID,
				payload:       payload,
				metadata:      msgMeta,
				createdAt:     createdOn,
				version:       version,
			},
			{
				title:         "message UUID is required",
				expectedError: aggregate.ErrMissingChangeUUID,
				aggregateID:   id,
				uuid:          messaging.UUID{},
				payload:       payload,
				metadata:      msgMeta,
				createdAt:     createdOn,
				version:       version,
			},
			{
				title:         "message payload is required",
				expectedError: aggregate.ErrInvalidChangePayload,
				aggregateID:   id,
				uuid:          messageID,
				payload:       nil,
				metadata:      msgMeta,
				createdAt:     createdOn,
				version:       version,
			},
			{
				title:         "message version is required",
				expectedError: aggregate.ErrInvalidChangeVersion,
				aggregateID:   id,
				uuid:          messageID,
				payload:       payload,
				metadata:      msgMeta,
				createdAt:     createdOn,
				version:       0,
			},
		}

		for _, test := range errorCases {
			t.Run(test.title, func(t *testing.T) {
				// Reconstitute Change message
				msg, err := aggregate.ReconstituteChange(
					test.aggregateID,
					test.uuid,
					test.payload,
					test.metadata,
					test.createdAt,
					test.version,
				)

				// Check error
				asserts := assert.New(t)
				asserts.Equal(test.expectedError, err, "Expected error")
				asserts.Empty(msg, "No message should be returned")
			})
		}
	})
}
