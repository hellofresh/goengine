package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/hellofresh/goengine/v2"
	driverSQL "github.com/hellofresh/goengine/v2/driver/sql"
)

var _ driverSQL.AggregateProjectorStorage = &AdvisoryLockAggregateProjectionStorage{}

// AdvisoryLockAggregateProjectionStorage is a AggregateProjectorStorage that uses an advisory locks to lock a projection
type AdvisoryLockAggregateProjectionStorage struct {
	stateSerialization driverSQL.ProjectionStateSerialization
	useLockField       bool

	logger goengine.Logger

	queryOutOfSyncProjections string
	queryPersistState         string
	queryPersistFailure       string
	queryAcquireLock          string
	queryReleaseLock          string
	querySetRowLocked         string
}

// NewAdvisoryLockAggregateProjectionStorage returns a new AdvisoryLockAggregateProjectionStorage
func NewAdvisoryLockAggregateProjectionStorage(
	eventStoreTable,
	projectionTable string,
	projectionStateSerialization driverSQL.ProjectionStateSerialization,
	useLockField bool,
	logger goengine.Logger,
) (*AdvisoryLockAggregateProjectionStorage, error) {
	switch {
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case strings.TrimSpace(eventStoreTable) == "":
		return nil, goengine.InvalidArgumentError("eventStoreTable")
	case projectionStateSerialization == nil:
		return nil, goengine.InvalidArgumentError("projectionStateSerialization")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)
	eventStoreTableQuoted := QuoteIdentifier(eventStoreTable)

	/* #nosec G201 */
	return &AdvisoryLockAggregateProjectionStorage{
		stateSerialization: projectionStateSerialization,
		useLockField:       useLockField,
		logger:             logger,

		queryOutOfSyncProjections: fmt.Sprintf(
			`WITH aggregate_position AS (
			   SELECT e.aggregate_id, MAX(e.no) AS no
		        FROM %[1]s AS e
			   GROUP BY aggregate_id
			 )
			 SELECT a.aggregate_id, a.no FROM aggregate_position AS a
			   LEFT JOIN %[2]s AS p ON p.aggregate_id = a.aggregate_id
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
		// queryAcquireLock uses a `WITH` in order to insert if the projection is unknown otherwise the row won't be locked
		// The reason for using `INSERT SELECT` instead of `INSERT VALUES ON CONFLICT DO NOTHING` is that `ON CONFLICT` will
		// increase the `no SERIAL` value.
		queryAcquireLock: fmt.Sprintf(
			`WITH projection AS (
				SELECT no, locked, failed, position, state FROM %[1]s WHERE aggregate_id = $1
			), new_projection AS (
			  INSERT INTO %[1]s (aggregate_id, state) SELECT $1, 'null' WHERE NOT EXISTS (
		    	 SELECT projection.no FROM projection
			  ) ON CONFLICT DO NOTHING
			  RETURNING *
			)
			SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, failed, position, state FROM new_projection
			UNION
			SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, failed, position, state FROM projection WHERE (position < $2 OR failed)`,
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

// LoadOutOfSync return a set of rows with the aggregate_id and number of the projection that are not in sync with the event store
func (a *AdvisoryLockAggregateProjectionStorage) LoadOutOfSync(ctx context.Context, conn driverSQL.Queryer) (*sql.Rows, error) {
	return conn.QueryContext(ctx, a.queryOutOfSyncProjections)
}

// PersistFailure marks the specified aggregate_id projection as failed
func (a *AdvisoryLockAggregateProjectionStorage) PersistFailure(conn driverSQL.Execer, notification *driverSQL.ProjectionNotification) error {
	_, err := conn.ExecContext(context.Background(), a.queryPersistFailure, notification.AggregateID)
	return err
}

// Acquire returns a driverSQL.ProjectorTransaction and the position of the projection within the event stream when a
// lock is acquired for the specified aggregate_id. Otherwise, an error is returned indicating why the lock could not be acquired.
func (a *AdvisoryLockAggregateProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (driverSQL.ProjectorTransaction, int64, error) {
	logFields := func(e goengine.LoggerEntry) {
		e.Int64("notification.no", notification.No)
		e.String("projection_id", notification.AggregateID)
	}
	aggregateID := notification.AggregateID

	res := conn.QueryRowContext(ctx, a.queryAcquireLock, aggregateID, notification.No)

	var (
		acquiredLock, locked, failed bool
		projectionState              driverSQL.ProjectionRawState
	)
	if err := res.Scan(&acquiredLock, &locked, &failed, &projectionState.Position, &projectionState.ProjectionState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, 0, driverSQL.ErrNoProjectionRequired
		}

		return nil, 0, err
	}

	if !acquiredLock {
		return nil, 0, driverSQL.ErrProjectionFailedToLock
	}

	if locked || failed {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case an application needs to decide what to do to avoid invalid projection states
		if err := a.releaseProjectionConnectionLock(conn, aggregateID); err != nil {
			a.logger.Error("failed to release lock for a projection with a locked row", func(e goengine.LoggerEntry) {
				logFields(e)
				e.Error(err)
			})
		} else {
			a.logger.Debug("released connection lock for a locked projection", logFields)
		}

		return nil, 0, driverSQL.ErrProjectionPreviouslyLocked
	}

	a.logger.Debug("acquired projection lock", logFields)

	tx := advisoryLockProjectorTransaction{
		conn:              conn,
		queryPersistState: a.queryPersistState,
		queryReleaseLock:  a.queryReleaseLock,

		stateSerialization: a.stateSerialization,
		rawState:           &projectionState,

		projectionID: aggregateID,
		logger:       a.logger,
	}

	if a.useLockField {
		return &advisoryLockWithUpdateProjectorTransaction{
			advisoryLockProjectorTransaction: tx,
			querySetRowLocked:                a.querySetRowLocked,
		}, projectionState.Position, nil
	}

	return &tx, projectionState.Position, nil
}

func (a *AdvisoryLockAggregateProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn, aggregateID string) error {
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
