package internal

import (
	"context"
	"database/sql/driver"
	"math"

	"github.com/hellofresh/goengine/eventstore/projector"
	"github.com/hellofresh/goengine/log"
	"github.com/pkg/errors"
)

type errorAction int

const (
	errorRetry errorAction = iota
	errorFail
	errorIgnore
	errorFallthrough
)

// WrapTriggerWithErrorHandler wrap the provided handler with error handling logic
func WrapTriggerWithErrorHandler(
	projectionCallback projector.ProjectionErrorCallback,
	handler Trigger,
	logger log.Logger,
) Trigger {
	return func(ctx context.Context, notification *projector.Notification) error {
		for i := 0; i < math.MaxInt16; i++ {
			err := handler(ctx, notification)
			if err == nil {
				return err
			}

			logger := logger.WithError(err).WithField("notification", notification)

			switch determineAction(projectionCallback, notification, err) {
			case errorRetry:
				logger.Debug("Trigger->ErrorHandler: retrying notification")
				continue
			case errorIgnore:
				logger.Debug("Trigger->ErrorHandler: ignoring error")
				return nil
			case errorFail, errorFallthrough:
				logger.Debug("Trigger->ErrorHandler: error fallthrough")
				return err
			}
		}

		return errors.Errorf(
			"seriously %d retries is enough! maybe it's time to fix your projection or error handling code?",
			math.MaxInt16,
		)
	}
}

// WrapProcessHandlerWithErrorHandler wrap the provided process handler with error handling logic
func WrapProcessHandlerWithErrorHandler(
	projectionCallback projector.ProjectionErrorCallback,
	handler ProcessHandler,
	markAsFailed func(ctx context.Context, notification *projector.Notification) error,
	logger log.Logger,
) ProcessHandler {
	return func(ctx context.Context, notification *projector.Notification, trigger Trigger) error {
		err := handler(ctx, notification, trigger)
		if err == nil {
			return err
		}

		logger := logger.WithError(err).WithField("notification", notification)

		switch determineAction(projectionCallback, notification, err) {
		case errorFail:
			logger.Debug("ProcessHandler->ErrorHandler: marking projection as failed")
			return markAsFailed(ctx, notification)
		case errorIgnore:
			logger.Debug("ProcessHandler->ErrorHandler: ignoring error")
			return nil
		case errorRetry:
			logger.Debug("ProcessHandler->ErrorHandler: re-queueing notification")
			return trigger(ctx, notification)
		}

		logger.Debug("ProcessHandler->ErrorHandler: error fallthrough")
		return err
	}
}

// determineAction determines the way the provided error should be handled
func determineAction(
	projectionCallback projector.ProjectionErrorCallback,
	notification *projector.Notification,
	err error,
) errorAction {
	switch err {
	case projector.ErrPreviouslyLocked:
		return errorFail
	case context.Canceled, projector.ErrNoProjectionRequired:
		return errorIgnore
	case driver.ErrBadConn, projector.ErrFailedToLock:
		return errorRetry
	default:
		switch e := err.(type) {
		case *projector.ProjectionHandlerError:
			switch projectionCallback(e.Cause(), notification) {
			case projector.ProjectionRetry:
				return errorRetry
			case projector.ProjectionIgnoreError:
				return errorIgnore
			case projector.ProjectionFail:
				return errorFail
			}
		}
	}

	return errorFallthrough
}
