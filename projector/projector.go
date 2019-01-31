package projector

import (
	"context"
)

type (
	// Projector is used to manage the execution of a projection
	Projector interface {
		// Run executes the projection
		Run(ctx context.Context) error

		// Run executes the projection and listen additions to the event store
		RunAndListen(ctx context.Context) error
	}

	// Notification is a representation of the data provided by postgres notify
	Notification struct {
		No          int64  `json:"no"`
		AggregateID string `json:"aggregate_id"`
	}

	// ProjectionErrorCallback is a function used to determin what action to take based on a failed projection
	ProjectionErrorCallback func(err error, notification *Notification) ErrorAction

	// ErrorAction a type containing the action that the projector should take after an error
	ErrorAction int
)

const (
	// ProjectionFail indicated that the projection failed and cannot be recovered
	// This means that a human as a service is needed
	ProjectionFail ErrorAction = iota
	// ProjectionRetry indicated that the notification should be retried
	// This can be used in combination with retry mechanism in the ProjectorErrorCallback
	ProjectionRetry ErrorAction = iota
	// ProjectionIgnoreError indicated that the projection failed but the failure can be ignored
	// This can be used when the ProjectorErrorCallback recovered the system from the error
	ProjectionIgnoreError ErrorAction = iota
)
