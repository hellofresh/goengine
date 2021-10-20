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

var _ driverSQL.StreamProjectorStorage = &AdvisoryLockStreamProjectionStorage{}

// AdvisoryLockStreamProjectionStorage is a StreamProjectorStorage that uses an advisory locks to lock a projection
type AdvisoryLockStreamProjectionStorage struct {
	projectionName               string
	projectionStateSerialization driverSQL.ProjectionStateSerialization
	useLockField                 bool

	logger goengine.Logger

	queryCreateProjection    string
	queryAcquireLock         string
	queryAcquirePositionLock string
	queryReleaseLock         string
	queryPersistState        string
	querySetRowLocked        string
}

// NewAdvisoryLockStreamProjectionStorage returns a new AdvisoryLockStreamProjectionStorage
func NewAdvisoryLockStreamProjectionStorage(
	projectionName,
	projectionTable string,
	projectionStateSerialization driverSQL.ProjectionStateSerialization,
	useLockField bool,
	logger goengine.Logger,
) (*AdvisoryLockStreamProjectionStorage, error) {
	switch {
	case strings.TrimSpace(projectionName) == "":
		return nil, goengine.InvalidArgumentError("projectionName")
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	case projectionStateSerialization == nil:
		return nil, goengine.InvalidArgumentError("projectionStateSerialization")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)

	/* #nosec G201 */
	return &AdvisoryLockStreamProjectionStorage{
		projectionName:               projectionName,
		projectionStateSerialization: projectionStateSerialization,
		useLockField:                 useLockField,
		logger:                       logger,

		queryCreateProjection: fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1) ON CONFLICT DO NOTHING`,
			projectionTableQuoted,
		),
		queryAcquireLock: fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, position, state FROM %[1]s WHERE name = $1`,
			projectionTableQuoted,
			projectionTableStr,
		),
		queryAcquirePositionLock: fmt.Sprintf(
			`SELECT pg_try_advisory_lock(%[2]s::regclass::oid::int, no), locked, position, state FROM %[1]s WHERE name = $1 AND position < $2`,
			projectionTableQuoted,
			projectionTableStr,
		),
		queryReleaseLock: fmt.Sprintf(
			`SELECT pg_advisory_unlock(%[2]s::regclass::oid::int, no) FROM %[1]s WHERE name = $1`,
			projectionTableQuoted,
			projectionTableStr,
		),
		queryPersistState: fmt.Sprintf(
			`UPDATE %[1]s SET position = $2, state = $3 WHERE name = $1`,
			projectionTableQuoted,
		),
		querySetRowLocked: fmt.Sprintf(
			`UPDATE ONLY %[1]s SET locked = $2 WHERE name = $1`,
			projectionTableQuoted,
		),
	}, nil
}

// CreateProjection creates the row in the projection table for the stream projection
func (s *AdvisoryLockStreamProjectionStorage) CreateProjection(ctx context.Context, conn driverSQL.Execer) error {
	_, err := conn.ExecContext(ctx, s.queryCreateProjection, s.projectionName)
	return err
}

// Acquire returns a driverSQL.ProjectorTransaction and the position of the projection within the event stream when a
// lock is acquire for the specified aggregate_id. Otherwise an error is returned indicating why the lock could not be acquired.
func (s *AdvisoryLockStreamProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (driverSQL.ProjectorTransaction, int64, error) {
	var (
		res       *sql.Row
		logFields func(e goengine.LoggerEntry)
	)
	if notification == nil {
		res = conn.QueryRowContext(ctx, s.queryAcquireLock, s.projectionName)
		logFields = func(e goengine.LoggerEntry) {
			e.Any("notification", nil)
		}
	} else {
		res = conn.QueryRowContext(ctx, s.queryAcquirePositionLock, s.projectionName, notification.No)
		logFields = func(e goengine.LoggerEntry) {
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		}
	}

	var (
		acquiredLock, locked bool
		projectionState      driverSQL.ProjectionRawState
	)
	if err := res.Scan(&acquiredLock, &locked, &projectionState.Position, &projectionState.ProjectionState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, 0, driverSQL.ErrNoProjectionRequired
		}

		return nil, 0, err
	}

	if !acquiredLock {
		return nil, 0, driverSQL.ErrProjectionFailedToLock
	}

	if locked {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := s.releaseProjectionConnectionLock(conn); err != nil {
			s.logger.Error("failed to release lock for a projection with a locked row", func(e goengine.LoggerEntry) {
				logFields(e)
				e.Error(err)
			})
		} else {
			s.logger.Debug("released connection lock for a locked projection", logFields)
		}

		return nil, 0, driverSQL.ErrProjectionPreviouslyLocked
	}

	s.logger.Debug("acquired projection lock", logFields)

	tx := advisoryLockProjectorTransaction{
		conn:              conn,
		queryPersistState: s.queryPersistState,
		queryReleaseLock:  s.queryReleaseLock,

		stateSerialization: s.projectionStateSerialization,
		rawState:           &projectionState,

		projectionID: s.projectionName,
		logger:       s.logger,
	}

	if s.useLockField {
		return &advisoryLockWithUpdateProjectorTransaction{
			advisoryLockProjectorTransaction: tx,
			querySetRowLocked:                s.querySetRowLocked,
		}, projectionState.Position, nil
	}

	return &tx, projectionState.Position, nil
}

func (s *AdvisoryLockStreamProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn) error {
	res := conn.QueryRowContext(context.Background(), s.queryReleaseLock, s.projectionName)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release projection connection lock")
	}

	return nil
}
