package projector

import (
	"errors"
	"fmt"
)

var (
	// ErrFailedToLock occurs when the projector cannot acquire the projection lock
	ErrFailedToLock = errors.New("unable to acquire projection lock")
	// ErrPreviouslyLocked occurs when a projection was lock was acquired but a previous lock is still in place
	ErrPreviouslyLocked = errors.New("unable to lock projection due to a previous lock being in place")
	// ErrNoProjectionRequired occurs when a notification was being acquired but the projection was already at the indicated position
	ErrNoProjectionRequired = errors.New("no projection acquisition required")

	_ error = &ProjectionHandlerError{}
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
	return fmt.Sprintf("the projection handler returned with an error. (%s)", e.error.Error())
}

// Cause returns the actual projection errors.
// This also adds support for github.com/pkg/errors.Cause
func (e *ProjectionHandlerError) Cause() error {
	return e.error
}
