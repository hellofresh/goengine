package internal

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/pkg/errors"
)

// Ensure the NotificationProjector.Execute is a ProjectionTrigger
var _ driverSQL.ProjectionTrigger = (&NotificationProjector{}).Execute

// NotificationProjector contains the logic for transforming a notification into a set of events and projecting them.
type NotificationProjector struct {
	db *sql.DB

	storage driverSQL.ProjectionStorage

	projectionStateInit   driverSQL.ProjectionStateInitializer
	projectionStateDecode driverSQL.ProjectionStateDecoder
	handlers              map[string]goengine.MessageHandler

	eventLoader driverSQL.EventStreamLoader
	resolver    goengine.MessagePayloadResolver

	logger goengine.Logger
}

// NewNotificationProjector returns a new NotificationProjector
func NewNotificationProjector(
	db *sql.DB,
	storage driverSQL.ProjectionStorage,
	projectionStateInit driverSQL.ProjectionStateInitializer,
	projectionStateDecode driverSQL.ProjectionStateDecoder,
	eventHandlers map[string]goengine.MessageHandler,
	eventLoader driverSQL.EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	logger goengine.Logger,
) (*NotificationProjector, error) {
	switch {
	case db == nil:
		return nil, goengine.InvalidArgumentError("db")
	case storage == nil:
		return nil, goengine.InvalidArgumentError("storage")
	case projectionStateInit == nil:
		return nil, goengine.InvalidArgumentError("projectionStateInit")
	case len(eventHandlers) == 0:
		return nil, goengine.InvalidArgumentError("eventHandlers")
	case eventLoader == nil:
		return nil, goengine.InvalidArgumentError("eventLoader")
	case resolver == nil:
		return nil, goengine.InvalidArgumentError("resolver")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	return &NotificationProjector{
		db:                    db,
		storage:               storage,
		projectionStateInit:   projectionStateInit,
		projectionStateDecode: projectionStateDecode,
		handlers:              wrapProjectionHandlers(eventHandlers),
		eventLoader:           eventLoader,
		resolver:              resolver,
		logger:                logger,
	}, nil
}

// Execute triggers the projections for the notification
func (s *NotificationProjector) Execute(ctx context.Context, notification *driverSQL.ProjectionNotification) error {
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
func (s *NotificationProjector) project(
	ctx context.Context,
	conn *sql.Conn,
	streamConn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) error {
	logger := s.logger.WithField("notification", notification)

	// Acquire the projection
	releaseLock, rawState, err := s.storage.Acquire(ctx, conn, notification)
	if err != nil {
		return err
	}
	defer releaseLock()

	// Decode or initialize projection state
	var projectionState interface{}
	if rawState.Position == 0 {
		// This is the fist time the projection runs so initialize the state
		projectionState, err = s.projectionStateInit(ctx)
		if err != nil {
			return err
		}
	} else if s.projectionStateDecode != nil {
		// Unmarshal the projection state
		projectionState, err = s.projectionStateDecode(rawState.ProjectionState)
		if err != nil {
			return err
		}
	}
	state := driverSQL.ProjectionState{
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
func (s *NotificationProjector) projectStream(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
	state driverSQL.ProjectionState,
	stream goengine.EventStream,
) error {
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
func wrapProjectionHandlers(handlers map[string]goengine.MessageHandler) map[string]goengine.MessageHandler {
	res := make(map[string]goengine.MessageHandler, len(handlers))
	for k, h := range handlers {
		res[k] = wrapProjectionHandlerToTrapError(h)
	}

	return res
}

// wrapProjectionHandlerToTrapError wraps a projection handler with error catching code.
// This ensures a projection handler can return a error or panic without destroying the executor
func wrapProjectionHandlerToTrapError(handler goengine.MessageHandler) goengine.MessageHandler {
	return func(ctx context.Context, state interface{}, message goengine.Message) (returnState interface{}, handlerErr error) {
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

			handlerErr = driverSQL.NewProjectionHandlerError(err)
		}()

		var err error
		returnState, err = handler(ctx, state, message)
		if err != nil {
			handlerErr = driverSQL.NewProjectionHandlerError(err)
		}

		return
	}
}
