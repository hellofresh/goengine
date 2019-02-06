package sql

import (
	"errors"
	"fmt"
)

const (
	// ProjectionFail indicated that the projection failed and cannot be recovered
	// This means that a human as a service is needed
	ProjectionFail ProjectionErrorAction = iota
	// ProjectionRetry indicated that the notification should be retried
	// This can be used in combination with retry mechanism in the ProjectorErrorCallback
	ProjectionRetry ProjectionErrorAction = iota
	// ProjectionIgnoreError indicated that the projection failed but the failure can be ignored
	// This can be used when the ProjectorErrorCallback recovered the system from the error
	ProjectionIgnoreError ProjectionErrorAction = iota
)

var (
	// ErrConnFailedToAcquire occurs when a connection cannot be acquired within the timelimit
	ErrConnFailedToAcquire = errors.New("goengine: unable to acquire projection lock")
	// ErrProjectionFailedToLock occurs when the projector cannot acquire the projection lock
	ErrProjectionFailedToLock = errors.New("goengine: unable to acquire projection lock")
	// ErrProjectionPreviouslyLocked occurs when a projection was lock was acquired but a previous lock is still in place
	ErrProjectionPreviouslyLocked = errors.New("goengine: unable to lock projection due to a previous lock being in place")
	// ErrNoProjectionRequired occurs when a notification was being acquired but the projection was already at the indicated position
	ErrNoProjectionRequired = errors.New("goengine: no projection acquisition required")
)

// ProjectionHandlerError an error indicating that a projection handler failed
type ProjectionHandlerError struct {
	error
}

// NewProjectionHandlerError return a ProjectionHandlerError with the cause being the provided error
func NewProjectionHandlerError(err error) *ProjectionHandlerError {
	return &ProjectionHandlerError{err}
}

// Error return the error message
func (e *ProjectionHandlerError) Error() string {
	return fmt.Sprintf("goengine: the projection handler returned with an error. (%s)", e.error.Error())
}

// Cause returns the actual projection errors.
// This also adds support for github.com/pkg/errors.Cause
func (e *ProjectionHandlerError) Cause() error {
	return e.error
}
