package goengine

import (
	"time"

	"github.com/google/uuid"

	"github.com/hellofresh/goengine/v2/metadata"
)

type (
	// UUID is a 128 bit (16 byte) Universal Unique Identifier as defined in RFC4122
	UUID = uuid.UUID

	// Message is an interface describing a message.
	// A message can be a command, domain event or some other type of message.
	Message interface {
		// UUID returns the identifier of this message
		UUID() UUID

		// CreatedAt returns the created time of the message
		CreatedAt() time.Time

		// Payload returns the payload of the message
		Payload() interface{}

		// Metadata return the message metadata
		Metadata() metadata.Metadata

		// WithMetadata Returns new instance of the message with key and value added to metadata
		WithMetadata(key string, value interface{}) Message
	}
)

// GenerateUUID creates a new random UUID or panics
func GenerateUUID() UUID {
	return uuid.New()
}

// IsUUIDEmpty returns true if the UUID is empty
func IsUUIDEmpty(id UUID) bool {
	return id == uuid.Nil
}
