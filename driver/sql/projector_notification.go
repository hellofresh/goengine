package sql

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
	"github.com/pkg/errors"
)

// Ensure the notificationProjector.Execute is a ProjectionTrigger
var _ ProjectionTrigger = (&notificationProjector{}).Execute

// notificationProjector contains the logic for transforming a notification into a set of events and projecting them.
type notificationProjector struct {
	db *sql.DB

	storage ProjectionStorage

	projectionStateInit   ProjectionStateInitializer
	projectionStateDecode ProjectionStateDecoder
	handlers              map[string]goengine.MessageHandler

	eventLoader EventStreamLoader
	resolver    goengine.MessagePayloadResolver

	logger goengine.Logger
}

// newNotificationProjector returns a new notificationProjector
func newNotificationProjector(
	db *sql.DB,
	storage ProjectionStorage,
	projectionStateInit ProjectionStateInitializer,
	projectionStateDecode ProjectionStateDecoder,
	eventHandlers map[string]goengine.MessageHandler,
	eventLoader EventStreamLoader,
	resolver goengine.MessagePayloadResolver,
	logger goengine.Logger,
) (*notificationProjector, error) {
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

	return &notificationProjector{
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
func (s *notificationProjector) Execute(ctx context.Context, notification *ProjectionNotification) error {
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
			s.logger.Warn("failed to db close project connection", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
		}
	}()

	streamConn, err := AcquireConn(ctx, s.db)
	if err != nil {
		return err
	}
	defer func() {
		if err := streamConn.Close(); err != nil {
			s.logger.Warn("failed to db close stream connection", func(e goengine.LoggerEntry) {
				e.Error(err)
			})
		}
	}()

	return s.project(ctx, streamConn, projectConn, notification)
}

// project acquires the needed projection based on the notification, unmarshal the state of the projection,
// loads the event stream and projects it.
func (s *notificationProjector) project(
	ctx context.Context,
	conn *sql.Conn,
	streamConn *sql.Conn,
	notification *ProjectionNotification,
) error {
	// Acquire the projection
	releaseLock, rawState, err := s.storage.Acquire(ctx, conn, notification)
	if err != nil {
		return err
	}
	defer releaseLock()

	// Load the event stream
	eventStream, err := s.eventLoader(ctx, streamConn, notification, rawState.Position)
	if err != nil {
		return err
	}
	defer func() {
		if err := eventStream.Close(); err != nil {
			s.logger.Warn("failed to close the event stream", func(e goengine.LoggerEntry) {
				e.Any("notification", notification)
				e.Error(err)
			})
		}
	}()

	// Wrap the eventStream
	handlerStream := &eventStreamHandlerIterator{
		stream:   eventStream,
		handlers: s.handlers,
		resolver: s.resolver,
	}

	// project event stream
	if err := s.projectStream(ctx, conn, notification, rawState, handlerStream); err != nil {
		return err
	}

	return eventStream.Close()
}

// projectStream will project the events in the event stream and persist the state after the projection
func (s *notificationProjector) projectStream(
	ctx context.Context,
	conn Execer,
	notification *ProjectionNotification,
	rawState *ProjectionRawState,
	stream *eventStreamHandlerIterator,
) error {
	var (
		err           error
		state         ProjectionState
		stateAcquired bool
	)
	for stream.Next() {
		// Check if the context is expired
		select {
		default:
		case <-ctx.Done():
			return nil
		}

		// Acquire the state if we have none
		if !stateAcquired {
			state, err = s.acquireProjectState(ctx, rawState)
			if err != nil {
				return err
			}
			stateAcquired = true
		}

		// Execute the handler
		state.Position = stream.MessageNumber()
		state.ProjectionState, err = stream.Project(ctx, state.ProjectionState)
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

func (s *notificationProjector) acquireProjectState(ctx context.Context, rawState *ProjectionRawState) (ProjectionState, error) {
	state := ProjectionState{
		Position: rawState.Position,
	}

	// Decode or initialize projection state
	var err error
	if rawState.Position == 0 {
		// This is the fist time the projection runs so initialize the state
		state.ProjectionState, err = s.projectionStateInit(ctx)
	} else if s.projectionStateDecode != nil {
		// Unmarshal the projection state
		state.ProjectionState, err = s.projectionStateDecode(rawState.ProjectionState)
	}

	return state, err
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

			handlerErr = NewProjectionHandlerError(err)
		}()

		var err error
		returnState, err = handler(ctx, state, message)
		if err != nil {
			handlerErr = NewProjectionHandlerError(err)
		}

		return
	}
}

// eventStreamHandlerIterator is a iterator used to project a support message
type eventStreamHandlerIterator struct {
	stream   goengine.EventStream
	handlers map[string]goengine.MessageHandler
	resolver goengine.MessagePayloadResolver

	message   goengine.Message
	position  int64
	eventName string
	err       error
}

func (s *eventStreamHandlerIterator) Next() bool {
	for {
		if !s.stream.Next() {
			return false
		}

		s.message, s.position, s.err = s.stream.Message()
		if s.err != nil {
			return false
		}

		// Resolve the payload event name
		s.eventName, s.err = s.resolver.ResolveName(s.message.Payload())
		if s.err != nil {
			return false
		}

		// Check if the event name has a payload handler
		if _, found := s.handlers[s.eventName]; found {
			return true
		}
	}
}

func (s *eventStreamHandlerIterator) MessageNumber() int64 {
	return s.position
}

func (s *eventStreamHandlerIterator) Project(ctx context.Context, state interface{}) (interface{}, error) {
	return s.handlers[s.eventName](ctx, state, s.message)
}

func (s *eventStreamHandlerIterator) Err() error {
	if s.err != nil {
		return s.err
	}

	return s.stream.Err()
}

func (s *eventStreamHandlerIterator) Close() error {
	err := s.stream.Close()

	s.handlers = nil
	s.resolver = nil
	s.message = nil
	s.stream = nil

	return err
}
