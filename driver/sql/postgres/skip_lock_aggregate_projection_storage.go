package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
)

var _ driverSQL.AggregateProjectorStorage = &SkipLockAggregateProjectionStorage{}

// SkipLockAggregateProjectionStorage is a AggregateProjectorStorage that uses a SKIP LOCKED method to lock a projection
type SkipLockAggregateProjectionStorage struct {
	projectionStateMarshaling driverSQL.ProjectionStateSerialization

	logger goengine.Logger

	queryOutOfSyncProjections string
	queryPersistState         string
	queryPersistFailure       string
	queryCreateRow            string
	queryAcquireRowLock       string
}

// NewSkipLockAggregateProjectionStorage returns a new SkipLockAggregateProjectionStorage
func NewSkipLockAggregateProjectionStorage(
	eventStoreTable,
	projectionTable string,
	projectionStateMarshaling driverSQL.ProjectionStateSerialization,
	logger goengine.Logger,
) (*SkipLockAggregateProjectionStorage, error) {
	switch {
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case strings.TrimSpace(eventStoreTable) == "":
		return nil, goengine.InvalidArgumentError("eventStoreTable")
	case projectionStateMarshaling == nil:
		return nil, goengine.InvalidArgumentError("projectionStateMarshaling")
	}
	if logger == nil {
		logger = goengine.NopLogger
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)
	eventStoreTableQuoted := QuoteIdentifier(eventStoreTable)

	/* #nosec G201 */
	return &SkipLockAggregateProjectionStorage{
		projectionStateMarshaling: projectionStateMarshaling,
		logger:                    logger,

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
		queryAcquireRowLock: fmt.Sprintf(
			`SELECT EXISTS(SELECT no FROM %[1]s WHERE aggregate_id = $1 FOR UPDATE SKIP LOCKED) AS acquiredLock, failed, position, state FROM %[1]s WHERE aggregate_id = $1`,
			projectionTableQuoted,
			projectionTableStr,
		),
		queryCreateRow: fmt.Sprintf(
			`INSERT INTO %[1]s (aggregate_id, state) 
				SELECT $1, 'null' WHERE NOT EXISTS (
					SELECT no FROM %[1]s WHERE aggregate_id = $1
			  	) ON CONFLICT DO NOTHING`,
			projectionTableQuoted,
		),
	}, nil
}

// LoadOutOfSync return a set of rows with the aggregate_id and number of the projection that are not in sync with the event store
func (a *SkipLockAggregateProjectionStorage) LoadOutOfSync(ctx context.Context, conn driverSQL.Queryer) (*sql.Rows, error) {
	return conn.QueryContext(ctx, a.queryOutOfSyncProjections)
}

// PersistFailure marks the specified aggregate_id projection as failed
func (a *SkipLockAggregateProjectionStorage) PersistFailure(conn driverSQL.Execer, notification *driverSQL.ProjectionNotification) error {
	if _, err := conn.ExecContext(context.Background(), a.queryPersistFailure, notification.AggregateID); err != nil {
		return err
	}

	return nil
}

// Acquire returns a driverSQL.ProjectorTransaction and the position of the projection within the event stream when a
// lock is acquire for the specified aggregate_id. Otherwise an error is returned indicating why the lock could not be acquired.
func (a *SkipLockAggregateProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (driverSQL.ProjectorTransaction, int64, error) {
	logFields := func(e goengine.LoggerEntry) {
		e.Int64("notification.no", notification.No)
		e.String("projection_id", notification.AggregateID)
	}
	aggregateID := notification.AggregateID

	transaction, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err != nil {
			if rollbackErr := transaction.Rollback(); rollbackErr != nil {
				a.logger.Error("acquire state could not rollback transaction", func(e goengine.LoggerEntry) {
					e.Error(rollbackErr)
				})
			}
		}
	}()

	var (
		acquiredLock    bool
		failed          bool
		projectionState driverSQL.ProjectionRawState
	)
	err = transaction.QueryRowContext(ctx, a.queryAcquireRowLock, aggregateID).
		Scan(&acquiredLock, &failed, &projectionState.Position, &projectionState.ProjectionState)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, 0, err
		}

		// No rows are returned this mean we need to create a new projection row
		_, err = conn.ExecContext(ctx, a.queryCreateRow, aggregateID)
		if err != nil {
			return nil, 0, err
		}

		err = transaction.QueryRowContext(ctx, a.queryAcquireRowLock, aggregateID).
			Scan(&acquiredLock, &failed, &projectionState.Position, &projectionState.ProjectionState)
		if err != nil {
			return nil, 0, err
		}
	}

	if projectionState.Position >= notification.No {
		return nil, 0, driverSQL.ErrNoProjectionRequired
	}

	if !acquiredLock {
		return nil, 0, driverSQL.ErrProjectionFailedToLock
	}

	if failed {
		return nil, 0, driverSQL.ErrProjectionPreviouslyLocked
	}

	a.logger.Debug("acquired projection lock", logFields)

	return &skipLockProjectorTransaction{
		conn:                conn,
		transaction:         transaction,
		queryAcquireRowLock: a.queryAcquireRowLock,
		queryPersistState:   a.queryPersistState,

		stateMarshaling: a.projectionStateMarshaling,
		rawState:        &projectionState,

		projectionID: aggregateID,
		logger:       a.logger,
	}, projectionState.Position, nil
}
