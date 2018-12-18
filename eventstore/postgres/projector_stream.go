package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/internal/log"
	"github.com/hellofresh/goengine/metadata"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	// ErrProjectionFailedToLock occurs when the projector cannot acquire the projection lock
	ErrProjectionFailedToLock = errors.New("unable to acquire projection lock")
	// ErrNoEventStore occurs when no event store is provided
	ErrNoEventStore = errors.New("no event store provided")
	// ErrNoPayloadResolver occurs when no payload resolver is provided
	ErrNoPayloadResolver = errors.New("no payload resolver provided")
	// ErrNoProjection occurs when no projection is provided
	ErrNoProjection = errors.New("no projection provided")
	// ErrNoProjectionTableName occurs when no projection table name is provided
	ErrNoProjectionTableName = errors.New("no projection table name provided")

	// Ensure that we satisfy the eventstore.Projector interface
	_ eventstore.Projector = &StreamProjector{}
)

type (
	// StreamProjector is a postgres projector used to execute a projection against an event stream.
	//
	// This projector uses postgres advisory locks (https://www.postgresql.org/docs/10/static/explicit-locking.html#ADVISORY-LOCKS)
	// to avoid projecting the same event multiple times.
	// Updates to the event stream are received by using the postgres notify and listen.
	StreamProjector struct {
		sync.Mutex

		projectorDB *projectorDB

		store           ReadOnlyEventStore
		resolver        eventstore.PayloadResolver
		projection      eventstore.Projection
		projectionTable string
		logger          logrus.FieldLogger

		eventHandlers map[string]eventstore.ProjectionHandler

		state    interface{}
		position int64
	}

	// ReadOnlyEventStore an interface describing a readonly event store that supports providing a SQL conn
	ReadOnlyEventStore interface {
		// LoadWithConnection returns a eventstream based on the provided constraints using the provided sql.Conn
		LoadWithConnection(ctx context.Context, conn *sql.Conn, streamName eventstore.StreamName, fromNumber int64, count *uint, metadataMatcher metadata.Matcher) (eventstore.EventStream, error)
	}
)

// NewStreamProjector creates a new projector for a projection
func NewStreamProjector(
	dbDSN string,
	store ReadOnlyEventStore,
	resolver eventstore.PayloadResolver,
	projection eventstore.Projection,
	projectionTable string,
	logger logrus.FieldLogger,
) (*StreamProjector, error) {
	switch {
	case dbDSN == "":
		return nil, ErrNoDBConnect
	case store == nil:
		return nil, ErrNoEventStore
	case resolver == nil:
		return nil, ErrNoPayloadResolver
	case projection == nil:
		return nil, ErrNoProjection
	case projectionTable == "":
		return nil, ErrNoProjectionTableName
	}

	if logger == nil {
		logger = log.NilLogger
	}
	logger = logger.WithFields(logrus.Fields{
		"projection":   projection.Name(),
		"event_stream": projection.FromStream(),
	})

	return &StreamProjector{
		projectorDB: &projectorDB{
			dbDSN:                dbDSN,
			dbChannel:            string(projection.FromStream()),
			minReconnectInterval: time.Millisecond,
			maxReconnectInterval: time.Second,
			logger:               logger,
		},
		store:           store,
		resolver:        resolver,
		projection:      projection,
		projectionTable: projectionTable,
		logger:          logger,
	}, nil
}

// Run executes the projection and manages the state of the projection
func (s *StreamProjector) Run(ctx context.Context, keepRunning bool) error {
	s.Lock()
	defer s.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	defer func() {
		if err := s.projectorDB.Close(); err != nil {
			s.logger.WithError(err).Error("failed to close projectorDB")
		}
	}()

	// Create the projection if none exists
	err := s.projectorDB.Exec(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if s.projectionExists(ctx, conn) {
			return nil
		}
		if err := s.createProjection(ctx, conn); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Trigger an initial run of the projection
	err = s.projectorDB.Trigger(ctx, s.project, nil)
	if err != nil {
		// If the projector needs to keep running but could not acquire a lock we still need to continue.
		if err == ErrProjectionFailedToLock && !keepRunning {
			return err
		}
	}
	if !keepRunning {
		return nil
	}

	return s.projectorDB.Listen(ctx, s.project)
}

// Reset trigger a reset of the projection and the projections state
func (s *StreamProjector) Reset(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	return s.projectorDB.Exec(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := s.projection.Reset(ctx); err != nil {
			return err
		}

		s.position = 0
		s.state = nil

		return s.persist(ctx, conn)
	})
}

// Delete removes the projection and the projections state
func (s *StreamProjector) Delete(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()

	return s.projectorDB.Exec(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := s.projection.Delete(ctx); err != nil {
			return err
		}

		_, err := conn.ExecContext(
			ctx,
			fmt.Sprintf(
				`DELETE FROM %s WHERE name = $1`,
				pq.QuoteIdentifier(s.projectionTable),
			),
			s.projection.Name(),
		)

		if err != nil {
			return err
		}

		return nil
	})
}

func (s *StreamProjector) project(ctx context.Context, projectConn *sql.Conn, streamConn *sql.Conn, notification *eventStoreNotification) error {
	if notification != nil && notification.No <= s.position {
		// The current position is a head of the notification so ignore
		s.logger.Debug("ignoring notification: it is behind the projection position")
		return nil
	}

	if err := s.acquireProjection(ctx, projectConn); err != nil {
		return err
	}
	defer func() {
		if err := s.releaseProjection(ctx, projectConn); err != nil {
			s.logger.WithError(err).Error("failed to release projection")
		}
	}()

	streamName := s.projection.FromStream()
	stream, err := s.store.LoadWithConnection(ctx, streamConn, streamName, s.position+1, nil, nil)
	if err != nil {
		return err
	}

	if err := s.handleStream(ctx, projectConn, streamName, stream); err != nil {
		if err := stream.Close(); err != nil {
			s.logger.WithError(err).Warn("failed to close the stream after a handling error occurred")
		}
		return err
	}

	if err := stream.Close(); err != nil {
		return err
	}

	return nil
}

func (s *StreamProjector) handleStream(ctx context.Context, conn *sql.Conn, streamName eventstore.StreamName, stream eventstore.EventStream) error {
	var msgCount int64
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
		msgCount++
		s.position = msgNumber

		// Resolve the payload event name
		eventName, err := s.resolver.ResolveName(msg.Payload())
		if err != nil {
			s.logger.WithField("payload", msg.Payload()).Debug("skipping event: unable to resolve payload name")
			continue
		}

		// Load event handlers if needed
		if s.eventHandlers == nil {
			s.eventHandlers = s.projection.Handlers()
		}

		// Resolve the payload handler using the event name
		handler, found := s.eventHandlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		s.state, err = handler(ctx, s.state, msg)
		if err != nil {
			return err
		}

		// Persist state and position changes
		if err := s.persist(ctx, conn); err != nil {
			return err
		}
	}

	return stream.Err()
}

func (s *StreamProjector) acquireProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), position, state FROM %s WHERE name = $1`,
			quoteString(s.projectionTable),
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)

	var (
		locked    bool
		jsonState []byte
		position  int64
	)
	if err := res.Scan(&locked, &position, &jsonState); err != nil {
		return err
	}

	if !locked {
		return ErrProjectionFailedToLock
	}

	var err error
	s.state, err = s.projection.ReconstituteState(jsonState)
	if err != nil {
		return err
	}
	s.position = position

	return nil
}

func (s *StreamProjector) releaseProjection(ctx context.Context, conn *sql.Conn) error {
	res := conn.QueryRowContext(
		ctx,
		fmt.Sprintf(
			`SELECT pg_advisory_unlock(%s::regclass::oid::int, no) FROM %s WHERE name = $1`,
			quoteString(s.projectionTable),
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release projection lock")
	}

	return nil
}

func (s *StreamProjector) persist(ctx context.Context, conn *sql.Conn) error {
	jsonState, err := json.Marshal(s.state)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`UPDATE %s SET position = $1, state = $2 WHERE name = $3`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.position,
		jsonState,
		s.projection.Name(),
	)
	if err != nil {
		return err
	}
	s.logger.WithField("position", s.position).Debug("updated projection state")

	return nil
}

func (s *StreamProjector) projectionExists(ctx context.Context, conn *sql.Conn) bool {
	rows, err := conn.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT 1 FROM %s WHERE name = $1 LIMIT 1`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)
	if err != nil {
		s.logger.WithField("table", s.projectionTable).WithError(err).Error("failed to query projection table")
		return false
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.logger.WithField("table", s.projectionTable).WithError(err).Warn("failed to close rows")
		}
	}()

	if !rows.Next() {
		return false
	}

	var found bool
	if err := rows.Scan(&found); err != nil {
		s.logger.WithField("table", s.projectionTable).WithError(err).Error("failed to scan projection table")
		return false
	}

	return found
}

func (s *StreamProjector) createProjection(ctx context.Context, conn *sql.Conn) error {
	// Ignore duplicate inserts. This can occur when multiple projectors are started at the same time.
	_, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1) ON CONFLICT DO NOTHING`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projection.Name(),
	)
	if err != nil {
		return err
	}

	return nil
}
