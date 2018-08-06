package aggregate

import (
	"time"

	"github.com/google/uuid"
)

var (
	// CurrentTime return the current UTC time
	CurrentTime = func() time.Time {
		return time.Now().UTC()
	}

	// GenerateID creates a new random UUID or panics
	GenerateID = func() ID {
		return ID(uuid.New().String())
	}
)

type (
	// ID an UUID for a aggregate.Root instance
	ID string
)
