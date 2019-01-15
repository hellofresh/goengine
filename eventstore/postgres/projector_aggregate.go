package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hellofresh/goengine/aggregate"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/internal/log"
	"github.com/hellofresh/goengine/metadata"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	// ErrNoEventStoreTable occurs when no event store table name is provided
	ErrNoEventStoreTable = errors.New("no event store table provided")
	// ErrEmptyAggregateTypeName occurs when an empty aggregate type name is provided
	ErrEmptyAggregateTypeName = errors.New("empty aggregate type name provided")

	// Ensure that we satisfy the eventstore.Projector interface
	_ eventstore.Projector = &AggregateProjector{}
)

type (
	// AggregateProjector is a postgres projector used to execute a projection per aggregate instance against an event stream
	AggregateProjector struct {
		sync.Mutex

		projectorDB *projectorDB

		eventStore        ReadOnlyEventStore
		eventStoreTable   string
		aggregateTypeName string
		resolver          eventstore.PayloadResolver
		projection        eventstore.Projection
		projectionTable   string
		eventHandlers     map[string]eventstore.ProjectionHandler
		logger            logrus.FieldLogger
	}

	// aggregateProjectorState contains the state of a single projection
	aggregateProjectorState struct {
		state    interface{}
		position int64
	}
)

// NewAggregateProjector creates a new projector for a projection
func NewAggregateProjector(
	dbDSN string,
	eventStore ReadOnlyEventStore,
	eventStoreTable string,
	resolver eventstore.PayloadResolver,
	aggregateTypeName string,
	projection eventstore.Projection,
	projectionTable string,
	logger logrus.FieldLogger,
) (*AggregateProjector, error) {
	switch {
	case dbDSN == "":
		return nil, ErrNoDBConnect
	case eventStore == nil:
		return nil, ErrNoEventStore
	case eventStoreTable == "":
		return nil, ErrNoEventStoreTable
	case resolver == nil:
		return nil, ErrNoPayloadResolver
	case projection == nil:
		return nil, ErrNoProjection
	case projectionTable == "":
		return nil, ErrNoProjectionTableName
	case aggregateTypeName == "":
		return nil, ErrEmptyAggregateTypeName
	}

	if logger == nil {
		logger = log.NilLogger
	}
	logger = logger.WithFields(logrus.Fields{
		"projection":   projection.Name(),
		"event_stream": projection.FromStream(),
	})

	return &AggregateProjector{
		projectorDB: &projectorDB{
			dbDSN:                dbDSN,
			dbChannel:            string(projection.FromStream()),
			minReconnectInterval: time.Millisecond,
			maxReconnectInterval: time.Second,
			logger:               logger,
		},
		eventStore:        eventStore,
		eventStoreTable:   eventStoreTable,
		aggregateTypeName: aggregateTypeName,
		resolver:          resolver,
		projection:        projection,
		projectionTable:   projectionTable,
		eventHandlers:     projection.Handlers(),
		logger:            logger,
	}, nil
}

// Run executes the projection and manages the state of the aggregate projections
func (a *AggregateProjector) Run(ctx context.Context, keepRunning bool) error {
	a.Lock()
	defer a.Unlock()

	// Check if the context is expired
	select {
	default:
	case <-ctx.Done():
		return nil
	}

	defer func() {
		if err := a.projectorDB.Close(); err != nil {
			a.logger.WithError(err).Error("failed to close projectorDB")
		}
	}()

	// TODO add check to ensure table exists

	if !keepRunning {
		// Trigger an single run of the projection
		return a.projectorDB.Trigger(ctx, a.project, nil)
	}

	return a.projectorDB.Listen(ctx, a.project)
}

func (a *AggregateProjector) project(ctx context.Context, projectConn *sql.Conn, streamConn *sql.Conn, notification *eventStoreNotification) error {
	if notification == nil {
		// Time to play catchup and check everything
		dirtyAggregatesQuery := fmt.Sprintf(
			`WITH aggregate_position AS (
			   SELECT e.metadata ->> '_aggregate_id' AS aggregate_id, MAX(e.no) AS no
		        FROM %[1]s AS e
			   GROUP BY aggregate_id
			 )
			 SELECT a.aggregate_id, a.no FROM aggregate_position AS a
			   LEFT JOIN %[2]s AS p ON p.aggregate_id::text = a.aggregate_id
			 WHERE p.aggregate_id IS NULL OR (a.no > p.position)`,
			pq.QuoteIdentifier(a.eventStoreTable),
			pq.QuoteIdentifier(a.projectionTable),
		)
		rows, err := projectConn.QueryContext(ctx, dirtyAggregatesQuery)
		if err != nil {
			return err
		}
		defer func() {
			if err := rows.Close(); err != nil {
				a.logger.WithError(err).Error("failed to close dirty rows")
			}
		}()

		for rows.Next() {
			// Check if the context is expired
			select {
			default:
			case <-ctx.Done():
				return nil
			}

			var (
				aggregateID string
				position    int64
			)

			if err := rows.Scan(&aggregateID, &position); err != nil {
				return err
			}

			// TODO run in parallel
			if err = a.projectorDB.Trigger(ctx, a.project, &eventStoreNotification{
				AggregateID: aggregateID,
				No:          position,
				EventName:   "",
			}); err != nil {
				return err
			}
		}

		if err := rows.Close(); err != nil {
			return err
		}

		return nil
	}

	return a.projectAggregate(ctx, streamConn, projectConn, notification.AggregateID)
}

func (a *AggregateProjector) projectAggregate(ctx context.Context, streamConn *sql.Conn, projectConn *sql.Conn, aggregateID string) error {
	info, err := a.acquireProjection(ctx, projectConn, aggregateID)
	if err != nil {
		return err
	}
	defer func() {
		if err := a.releaseProjection(projectConn, aggregateID); err != nil {
			a.logger.
				WithField("projection_aggregate", aggregateID).
				WithError(err).
				Error("failed to release projection")
		}
	}()

	matcher := metadata.NewMatcher()
	matcher = metadata.WithConstraint(matcher, aggregate.IDKey, metadata.Equals, aggregateID)
	matcher = metadata.WithConstraint(matcher, aggregate.TypeKey, metadata.Equals, a.aggregateTypeName)

	streamName := a.projection.FromStream()
	stream, err := a.eventStore.LoadWithConnection(ctx, streamConn, streamName, info.position+1, nil, matcher)
	if err != nil {
		return err
	}

	if err := a.handleStream(ctx, projectConn, streamName, stream, aggregateID, info); err != nil {
		if err := stream.Close(); err != nil {
			a.logger.
				WithField("projection_aggregate", aggregateID).
				WithError(err).
				Warn("failed to close the stream after a handling error occurred")
		}
		return err
	}

	if err := stream.Close(); err != nil {
		return err
	}

	return nil
}

func (a *AggregateProjector) handleStream(
	ctx context.Context,
	conn *sql.Conn,
	streamName eventstore.StreamName,
	stream eventstore.EventStream,
	aggregateID string,
	projectionInfo *aggregateProjectorState,
) error {
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
		projectionInfo.position = msgNumber

		// Resolve the payload event name
		eventName, err := a.resolver.ResolveName(msg.Payload())
		if err != nil {
			a.logger.WithFields(logrus.Fields{
				"payload":              msg.Payload(),
				"projection_aggregate": aggregateID,
			}).Debug("skipping event: unable to resolve payload name")
			continue
		}

		// Resolve the payload handler using the event name
		handler, found := a.eventHandlers[eventName]
		if !found {
			continue
		}

		// Execute the handler
		projectionInfo.state, err = handler(ctx, projectionInfo.state, msg)
		if err != nil {
			return err
		}

		// Persist state and position changes
		if err := a.persist(conn, aggregateID, projectionInfo); err != nil {
			return err
		}
	}

	return stream.Err()
}

func (a *AggregateProjector) persist(conn *sql.Conn, aggregateID string, projectionInfo *aggregateProjectorState) error {
	jsonState, err := json.Marshal(projectionInfo.state)
	if err != nil {
		return err
	}

	updateQuery := fmt.Sprintf(
		`UPDATE %s SET position = $2, state = $3 WHERE aggregate_id = $1`,
		pq.QuoteIdentifier(a.projectionTable),
	)

	_, err = conn.ExecContext(context.Background(), updateQuery, aggregateID, projectionInfo.position, jsonState)
	if err != nil {
		return err
	}

	a.logger.WithFields(logrus.Fields{
		"projection_aggregate": aggregateID,
		"projection_info":      projectionInfo,
	}).Debug("updated projection state")
	return nil
}

func (a *AggregateProjector) acquireProjection(ctx context.Context, conn *sql.Conn, aggregateID string) (*aggregateProjectorState, error) {
	// We use a with in order to insert if the projection is unknown other wise the row won't be locked
	// The reason for using `INSERT SELECT` instead of `INSERT VALUES ON CONFLICT DO NOTHING` is that `ON CONFLICT` will
	// increase the `no SERIAL` value.
	acquireLockQuery := fmt.Sprintf(
		`WITH new_projection AS (
		  INSERT INTO %[1]s (aggregate_id, state) SELECT $1, 'null' WHERE NOT EXISTS (
		    SELECT * FROM %[1]s WHERE aggregate_id = $1
		  )
		  RETURNING *
		)
		SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, position, state FROM new_projection
		UNION
		SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, position, state FROM %[1]s WHERE aggregate_id = $1`,
		pq.QuoteIdentifier(a.projectionTable),
		quoteString(a.projectionTable),
	)
	res := conn.QueryRowContext(ctx, acquireLockQuery, aggregateID)

	var (
		acquiredLock bool
		locked       bool
		jsonState    []byte
		position     int64
	)
	if err := res.Scan(&acquiredLock, &locked, &position, &jsonState); err != nil {
		return nil, err
	}

	if !acquiredLock {
		return nil, ErrProjectionFailedToLock
	}

	if locked {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := a.releaseProjectionConnectionLock(conn, aggregateID); err != nil {
			a.logger.WithError(err).Error("failed to release lock for a projection with a locked row")
		}

		return nil, ErrProjectionPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE ONLY %[1]s SET locked = TRUE WHERE aggregate_id = $1`, pq.QuoteIdentifier(a.projectionTable)),
		aggregateID,
	)
	if err != nil {
		if err := a.releaseProjection(conn, aggregateID); err != nil {
			a.logger.WithError(err).Error("failed to release lock while setting projection rows as locked")
		}

		return nil, err
	}

	state, err := a.projection.ReconstituteState(jsonState)
	if err != nil {
		if err := a.releaseProjection(conn, aggregateID); err != nil {
			a.logger.WithError(err).Error("failed to release lock after ReconstituteState failure")
		}

		return nil, err
	}

	return &aggregateProjectorState{
		state:    state,
		position: position,
	}, nil
}

func (a *AggregateProjector) releaseProjection(conn *sql.Conn, aggregateID string) error {
	res := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf(
			`UPDATE ONLY %[2]s SET locked = FALSE WHERE aggregate_id = $1 
			 RETURNING pg_advisory_unlock(%[1]s::regclass::oid::int, no)`,
			quoteString(a.projectionTable),
			pq.QuoteIdentifier(a.projectionTable),
		),
		aggregateID,
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

func (a *AggregateProjector) releaseProjectionConnectionLock(conn *sql.Conn, aggregateID string) error {
	res := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf(
			`SELECT pg_advisory_unlock(%s::regclass::oid::int, no) FROM %s WHERE aggregate_id = $1`,
			quoteString(a.projectionTable),
			pq.QuoteIdentifier(a.projectionTable),
		),
		aggregateID,
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
