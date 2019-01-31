package snapshot

import "time"

// Snapshot ...
type Snapshot struct {
	version     int
	aggregateID string
	// aggregate
	createdAt time.Time
}
