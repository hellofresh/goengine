package internal

import (
	"context"
	"database/sql"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/log"
	"github.com/hellofresh/goengine/projector"
	"github.com/pkg/errors"
)

var _ Trigger = (&NotificationProjector{}).Execute

// NotificationProjector contains the logic for transforming a notification into a set of events and projecting them.
type NotificationProjector struct {
	db *sql.DB

	storage Storage

	acquireUnmarshalState UnmarshalAcquiredState
	handlers              map[string]goengine_dev.MessageHandler

	eventLoader EventStreamLoader
	resolver    goengine_dev.MessagePayloadResolver

	logger goengine_dev.Logger
}

// NewNotificationProjector returns a new NotificationProjector
func NewNotificationProjector(
	db *sql.DB,
	storage Storage,
	acquireUnmarshalState UnmarshalAcquiredState,
	eventHandlers map[string]goengine_dev.MessageHandler,
	eventLoader EventStreamLoader,
	resolver goengine_dev.MessagePayloadResolver,
	logger goengine_dev.Logger,
) (*NotificationProjector, error) {
	switch {
	case db == nil:
		return nil, errors.New("db cannot be nil")
	case storage == nil:
		return nil, errors.New("storage cannot be nil")
	case acquireUnmarshalState == nil:
		return nil, errors.New("acquireUnmarshalState cannot be nil")
	case len(eventHandlers) == 0:
		return nil, errors.New("eventHandlers cannot be empty")
	case eventLoader == nil:
		return nil, errors.New("eventLoader cannot be nil")
	case resolver == nil:
		return nil, errors.New("resolver cannot be nil")
	}

	if logger == nil {
		logger = log.NilLogger
	}

	return &NotificationProjector{
		db:                    db,
		storage:               storage,
		acquireUnmarshalState: acquireUnmarshalState,
		handlers:              wrapProjectionHandlers(eventHandlers),
		eventLoader:           eventLoader,
		resolver:              resolver,
		logger:                logger,
	}, nil
}

// Execute triggers the projections for the notification
func (s *NotificationProjector) Execute(ctx context.Context, notification *projector.Notification) error {
	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	projectConn, err := AcquireConn(ctx, s.db)
	if err != nil {
		return err
	}
	defer func() {
		if err := projectConn.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to db close project connection")
		}
	}()

	streamConn, err := AcquireConn(ctx, s.db)
	if err != nil {
		return err
	}
	defer func() {
		if err := streamConn.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to db close stream connection")
		}
	}()

	return s.project(ctx, streamConn, projectConn, notification)
}

// project acquires the needed projection based on the notification, unmarshal the state of the projection,
// loads the event stream and projects it.
func (s *NotificationProjector) project(ctx context.Context, conn *sql.Conn, streamConn *sql.Conn, notification *projector.Notification) error {
	logger := s.logger.WithField("notification", notification)

	// Acquire the projection
	releaseLock, rawState, err := s.storage.Acquire(ctx, conn, notification)
	if err != nil {
		return err
	}
	defer releaseLock()

	// Unmarshal the projection state
	projectionState, err := s.acquireUnmarshalState(rawState.ProjectionState)
	if err != nil {
		return err
	}
	state := State{
		Position:        rawState.Position,
		ProjectionState: projectionState,
	}

	// Load the event stream
	eventStream, err := s.eventLoader(ctx, streamConn, notification, state)
	if err != nil {
		return err
	}
	defer func() {
		if err := eventStream.Close(); err != nil {
			logger.WithError(err).Warn("failed to close the event stream")
		}
	}()

	// project event stream
	if err := s.projectStream(ctx, conn, notification, state, eventStream); err != nil {
		return err
	}

	return eventStream.Close()
}

// projectStream will project the events in the event stream and persist the state after the projection
func (s *NotificationProjector) projectStream(ctx context.Context, conn *sql.Conn, notification *projector.Notification, state State, stream goengine_dev.EventStream) error {
	for stream.Next() {
		// Check if the context is expired
		select {
		default:
		case <-ctx.Done():
			return nil
		}

		// Get the message
		msg, msgNumber, err := stream.Message()
		if err != nil {
			return err
		}
		state.Position = msgNumber

		// Resolve the payload event name
		eventName, err := s.resolver.ResolveName(msg.Payload())
		if err != nil {
			s.logger.
				WithField("payload", msg.Payload()).
				Warn("skipping event: unable to resolve payload name")
			continue
		}

		// Resolve the payload handler using the event name
		handler, found := s.handlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		state.ProjectionState, err = handler(ctx, state.ProjectionState, msg)
		if err != nil {
			return err
		}

		// Persist state and position changes
		if err := s.storage.PersistState(conn, notification, state); err != nil {
			return err
		}
	}

	return stream.Err()
}

// wrapProjectionHandlers wraps the projection handlers so that any error or panic is caught and returned
func wrapProjectionHandlers(handlers map[string]goengine_dev.MessageHandler) map[string]goengine_dev.MessageHandler {
	res := make(map[string]goengine_dev.MessageHandler, len(handlers))
	for k, h := range handlers {
		res[k] = wrapProjectionHandlerToTrapError(h)
	}

	return res
}

// wrapProjectionHandlerToTrapError wraps a projection handler with error catching code.
// This ensures a projection handler can return a error or panic without destroying the executor
func wrapProjectionHandlerToTrapError(handler goengine_dev.MessageHandler) goengine_dev.MessageHandler {
	return func(ctx context.Context, state interface{}, message goengine_dev.Message) (returnState interface{}, handlerErr error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}

			// find out exactly what the error was and set err
			var err error
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.Errorf("unknown panic: (%T) %v", x, x)
			}

			handlerErr = projector.NewProjectionHandlerError(err)
		}()

		var err error
		returnState, err = handler(ctx, state, message)
		if err != nil {
			handlerErr = projector.NewProjectionHandlerError(err)
		}

		return
	}
}
