package postgres

import (
	"context"
	"database/sql/driver"

	"github.com/hellofresh/goengine/driver/sql"
)

type errorAction int

const (
	errorRetry errorAction = iota
	errorFail
	errorIgnore
	errorFallthrough
)

// resolveErrorAction determines the way the provided error should be handled
func resolveErrorAction(
	projectionCallback sql.ProjectionErrorCallback,
	notification *sql.ProjectionNotification,
	err error,
) errorAction {
	switch err {
	case sql.ErrProjectionPreviouslyLocked:
		return errorFail
	case context.Canceled, sql.ErrNoProjectionRequired:
		return errorIgnore
	case driver.ErrBadConn, sql.ErrConnFailedToAcquire, sql.ErrProjectionFailedToLock:
		return errorRetry
	default:
		switch e := err.(type) {
		case *sql.ProjectionHandlerError:
			switch projectionCallback(e.Cause(), notification) {
			case sql.ProjectionRetry:
				return errorRetry
			case sql.ProjectionIgnoreError:
				return errorIgnore
			case sql.ProjectionFail:
				return errorFail
			}
		}
	}

	return errorFallthrough
}
