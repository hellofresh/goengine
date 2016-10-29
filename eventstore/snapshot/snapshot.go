package snapshot

import "time"

type Snapshot struct {
	version     int
	aggregateID string
	// aggregate
	createdAt time.Time
}
