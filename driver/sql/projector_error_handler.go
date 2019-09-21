package sql

import (
	"context"
	"database/sql/driver"
)

type errorAction int

const (
	errorRetry errorAction = iota
	errorFail
	errorIgnore
	errorFallthrough
)

// resolveErrorAction determines the way the provided error should be handled by the projector
func resolveErrorAction(
	projectionCallback ProjectionErrorCallback,
	notification *ProjectionNotification,
	err error,
) errorAction {
	switch err {
	case ErrProjectionPreviouslyLocked:
		return errorFail
	case context.Canceled, ErrNoProjectionRequired:
		return errorIgnore
	case driver.ErrBadConn, ErrConnFailedToAcquire, ErrProjectionFailedToLock:
		return errorRetry
	default:
		switch e := err.(type) {
		case *ProjectionHandlerError:
			switch projectionCallback(e.Cause(), notification) {
			case ProjectionRetry:
				return errorRetry
			case ProjectionIgnoreError:
				return errorIgnore
			case ProjectionFail:
				return errorFail
			}
		}
	}

	return errorFallthrough
}
