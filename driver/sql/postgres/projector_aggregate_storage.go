package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/metadata"
	"github.com/pkg/errors"
)

func aggregateProjectionEventStreamLoader(eventStore driverSQL.ReadOnlyEventStore, streamName goengine.StreamName, aggregateTypeName string) driverSQL.EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *driverSQL.ProjectionNotification, state driverSQL.ProjectionState) (goengine.EventStream, error) {
		matcher := metadata.NewMatcher()
		matcher = metadata.WithConstraint(matcher, aggregate.IDKey, metadata.Equals, notification.AggregateID)
		matcher = metadata.WithConstraint(matcher, aggregate.TypeKey, metadata.Equals, aggregateTypeName)

		return eventStore.LoadWithConnection(ctx, conn, streamName, state.Position+1, nil, matcher)
	}
}

var _ driverSQL.ProjectionStorage = &aggregateProjectionStorage{}

type aggregateProjectionStorage struct {
	projectionStateEncoder driverSQL.ProjectionStateEncoder

	logger goengine.Logger

	queryOutOfSyncProjections string
	queryPersistState         string
	queryPersistFailure       string
	queryAcquireLock          string
	queryReleaseLock          string
	querySetRowLocked         string
}

func newAggregateProjectionStorage(
	eventStoreTable,
	projectionTable string,
	projectionStateEncoder driverSQL.ProjectionStateEncoder,
	logger goengine.Logger,
) (*aggregateProjectionStorage, error) {
	switch {
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case strings.TrimSpace(eventStoreTable) == "":
		return nil, goengine.InvalidArgumentError("eventStoreTable")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}
	if projectionStateEncoder == nil {
		projectionStateEncoder = defaultProjectionStateEncoder
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)
	eventStoreTableQuoted := QuoteIdentifier(eventStoreTable)

	/* #nosec */
	return &aggregateProjectionStorage{
		projectionStateEncoder: projectionStateEncoder,
		logger:                 logger,

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
			  ) ON CONFLICT DO NOTHING
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
	}, nil
}

func (a *aggregateProjectionStorage) LoadOutOfSync(ctx context.Context, conn *sql.Conn) (*sql.Rows, error) {
	return conn.QueryContext(ctx, a.queryOutOfSyncProjections)
}

func (a *aggregateProjectionStorage) PersistState(conn *sql.Conn, notification *driverSQL.ProjectionNotification, state driverSQL.ProjectionState) error {
	encodedState, err := a.projectionStateEncoder(state.ProjectionState)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), a.queryPersistState, notification.AggregateID, state.Position, encodedState)
	if err != nil {
		return err
	}

	a.logger.WithFields(goengine.Fields{
		"notification": notification,
		"state":        state,
	}).Debug("updated projection state")
	return nil
}

func (a *aggregateProjectionStorage) PersistFailure(ctx context.Context, conn *sql.Conn, notification *driverSQL.ProjectionNotification) error {
	if _, err := conn.ExecContext(ctx, a.queryPersistFailure, notification.AggregateID); err != nil {
		return err
	}

	return nil
}

func (a *aggregateProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (func(), *driverSQL.ProjectionRawState, error) {
	logger := a.logger.WithField("notification", notification)
	aggregateID := notification.AggregateID

	res := conn.QueryRowContext(ctx, a.queryAcquireLock, aggregateID, notification.No)

	var (
		acquiredLock bool
		locked       bool
		failed       bool
		rawState     []byte
		position     int64
	)
	if err := res.Scan(&acquiredLock, &locked, &failed, &position, &rawState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, nil, driverSQL.ErrNoProjectionRequired
		}

		return nil, nil, err
	}

	if !acquiredLock {
		return nil, nil, driverSQL.ErrProjectionFailedToLock
	}

	if locked || failed {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := a.releaseProjectionConnectionLock(conn, aggregateID); err != nil {
			logger.WithError(err).Error("failed to release lock for a projection with a locked row")
		} else {
			logger.Debug("released connection lock for a locked projection")
		}

		return nil, nil, driverSQL.ErrProjectionPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(ctx, a.querySetRowLocked, aggregateID, true)
	if err != nil {
		if releaseErr := a.releaseProjection(conn, aggregateID); releaseErr != nil {
			logger.WithError(releaseErr).Error("failed to release lock while setting projection rows as locked")
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
	}, &driverSQL.ProjectionRawState{ProjectionState: rawState, Position: position}, nil
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
