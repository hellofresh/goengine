package aggregate

import "github.com/google/uuid"

type (
	// ID an UUID for a aggregate.Root instance
	ID string
)

// GenerateID creates a new random UUID or panics
func GenerateID() ID {
	return ID(uuid.New().String())
}
