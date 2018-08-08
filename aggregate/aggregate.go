package aggregate

import (
	"github.com/google/uuid"
)

var (
	// GenerateID creates a new random UUID or panics
	GenerateID = func() ID {
		return ID(uuid.New().String())
	}
)

type (
	// ID an UUID for a aggregate.Root instance
	ID string
)
