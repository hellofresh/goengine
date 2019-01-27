package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/projector"
	"github.com/hellofresh/goengine/eventstore/projector/internal"
	eventStoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/hellofresh/goengine/metadata"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func aggregateProjectionEventStreamLoader(eventStore eventStoreSQL.ReadOnlyEventStore, streamName eventstore.StreamName, aggregateTypeName string) internal.EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *projector.Notification, state internal.State) (eventstore.EventStream, error) {
		matcher := metadata.NewMatcher()
		matcher = metadata.WithConstraint(matcher, aggregate.IDKey, metadata.Equals, notification.AggregateID)
		matcher = metadata.WithConstraint(matcher, aggregate.TypeKey, metadata.Equals, aggregateTypeName)

		return eventStore.LoadWithConnection(ctx, conn, streamName, state.Position+1, nil, matcher)
	}
}

var _ internal.Storage = &aggregateProjectionStorage{}

type aggregateProjectionStorage struct {
	logger logrus.FieldLogger

	queryOutOfSyncProjections string
	queryPersistState         string
	queryPersistFailure       string
	queryAcquireLock          string
	queryReleaseLock          string
	querySetRowLocked         string
}

func newAggregateProjectionStorage(
	projectionTable,
	eventStoreTable string,
	logger logrus.FieldLogger,
) *aggregateProjectionStorage {
	projectionTableQuoted := pq.QuoteIdentifier(projectionTable)
	projectionTableStr := quoteString(projectionTable)
	eventStoreTableQuoted := pq.QuoteIdentifier(eventStoreTable)

	return &aggregateProjectionStorage{
		logger: logger,

		queryOutOfSyncProjections: fmt.Sprintf(
			`WITH aggregate_position AS (
			   SELECT e.metadata ->> '_aggregate_id' AS aggregate_id, MAX(e.no) AS no
		        FROM %[1]s AS e
			   GROUP BY aggregate_id
			 )
			 SELECT a.aggregate_id, a.no FROM aggregate_position AS a
			   LEFT JOIN %[2]s AS p ON p.aggregate_id::text = a.aggregate_id
			 WHERE p.aggregate_id IS NULL OR (a.no > p.position)`,
			eventStoreTableQuoted,
			projectionTableQuoted,
		),
		queryPersistState: fmt.Sprintf(
			`UPDATE %[1]s SET position = $2, state = $3 WHERE aggregate_id = $1`,
			projectionTableQuoted,
		),
		queryPersistFailure: fmt.Sprintf(
			`UPDATE %[1]s SET failed = TRUE WHERE aggregate_id = $1`,
			projectionTableQuoted,
		),
		// queryAcquireLock uses a `WITH` in order to insert if the projection is unknown other wise the row won't be locked
		// The reason for using `INSERT SELECT` instead of `INSERT VALUES ON CONFLICT DO NOTHING` is that `ON CONFLICT` will
		// increase the `no SERIAL` value.
		queryAcquireLock: fmt.Sprintf(
			`WITH new_projection AS (
			  INSERT INTO %[1]s (aggregate_id, state) SELECT $1, 'null' WHERE NOT EXISTS (
		    	SELECT * FROM %[1]s WHERE aggregate_id = $1
			  )
			  RETURNING *
			)
			SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, failed, position, state FROM new_projection
			UNION
			SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, failed, position, state FROM %[1]s WHERE aggregate_id = $1 AND (position < $2 OR failed)`,
			projectionTableQuoted,
			projectionTableStr,
		),
		queryReleaseLock: fmt.Sprintf(
			`SELECT pg_advisory_unlock(%[2]s::regclass::oid::int, no) FROM %[1]s WHERE aggregate_id = $1`,
			projectionTableQuoted,
			projectionTableStr,
		),
		querySetRowLocked: fmt.Sprintf(
			`UPDATE ONLY %[1]s SET locked = $2 WHERE aggregate_id = $1`,
			projectionTableQuoted,
		),
	}
}

func (a *aggregateProjectionStorage) LoadOutOfSync(ctx context.Context, conn *sql.Conn) (*sql.Rows, error) {
	return conn.QueryContext(ctx, a.queryOutOfSyncProjections)
}

func (a *aggregateProjectionStorage) PersistState(conn *sql.Conn, notification *projector.Notification, state internal.State) error {
	jsonState, err := json.Marshal(state.ProjectionState)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), a.queryPersistState, notification.AggregateID, state.Position, jsonState)
	if err != nil {
		return err
	}

	a.logger.WithFields(logrus.Fields{
		"state":        state,
		"notification": notification,
	}).Debug("updated projection state")
	return nil
}

func (a *aggregateProjectionStorage) PersistFailure(ctx context.Context, conn *sql.Conn, notification *projector.Notification) error {
	if _, err := conn.ExecContext(ctx, a.queryPersistFailure, notification.AggregateID); err != nil {
		return err
	}

	return nil
}

func (a *aggregateProjectionStorage) Acquire(ctx context.Context, conn *sql.Conn, notification *projector.Notification) (func(), *internal.AcquiredState, error) {
	logger := a.logger.WithField("notification", notification)
	aggregateID := notification.AggregateID

	res := conn.QueryRowContext(ctx, a.queryAcquireLock, aggregateID, notification.No)

	var (
		acquiredLock bool
		locked       bool
		failed       bool
		jsonState    []byte
		position     int64
	)
	if err := res.Scan(&acquiredLock, &locked, &failed, &position, &jsonState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, nil, projector.ErrNoProjectionRequired
		}

		// It can happen that we receive a unique_violation error meaning that the row was inserted by another
		// process but postgres was already running the current insert. We can safely ignore this error.
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			logger.WithError(err).Warn("duplicate projection insert, ignoring Acquire call")
			return nil, nil, projector.ErrFailedToLock
		}

		return nil, nil, err
	}

	if !acquiredLock {
		return nil, nil, projector.ErrFailedToLock
	}

	if locked || failed {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := a.releaseProjectionConnectionLock(conn, aggregateID); err != nil {
			logger.WithError(err).Error("failed to release lock for a projection with a locked row")
		} else {
			logger.Debug("released connection lock for a locked projection")
		}

		return nil, nil, projector.ErrPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(ctx, a.querySetRowLocked, aggregateID, true)
	if err != nil {
		if err := a.releaseProjection(conn, aggregateID); err != nil {
			logger.WithError(err).Error("failed to release lock while setting projection rows as locked")
		} else {
			logger.Debug("failed to set projection as locked")
		}

		return nil, nil, err
	}
	logger.Debug("acquired projection lock")

	return func() {
		if err := a.releaseProjection(conn, aggregateID); err != nil {
			logger.WithError(err).Error("failed to release projection lock")
		} else {
			logger.Debug("released projection lock")
		}
	}, &internal.AcquiredState{ProjectionState: jsonState, Position: position}, nil
}

func (a *aggregateProjectionStorage) releaseProjection(conn *sql.Conn, aggregateID string) error {
	// Set the projection as row unlocked
	_, err := conn.ExecContext(context.Background(), a.querySetRowLocked, aggregateID, false)
	if err != nil {
		return err
	}

	return a.releaseProjectionConnectionLock(conn, aggregateID)
}

func (a *aggregateProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn, aggregateID string) error {
	res := conn.QueryRowContext(context.Background(), a.queryReleaseLock, aggregateID)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release db connection projection lock")
	}

	return nil
}
