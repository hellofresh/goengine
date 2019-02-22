package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/hellofresh/goengine"

	driverSQL "github.com/hellofresh/goengine/driver/sql"
)

func streamProjectionEventStreamLoader(eventStore driverSQL.ReadOnlyEventStore, streamName goengine.StreamName) driverSQL.EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *driverSQL.ProjectionNotification, state driverSQL.ProjectionState) (goengine.EventStream, error) {
		return eventStore.LoadWithConnection(ctx, conn, streamName, state.Position+1, nil, nil)
	}
}

var _ driverSQL.ProjectionStorage = &streamProjectionStorage{}

type streamProjectionStorage struct {
	projectionName         string
	projectionStateEncoder driverSQL.ProjectionStateEncoder

	logger goengine.Logger

	queryAcquireLock         string
	queryAcquirePositionLock string
	queryReleaseLock         string
	queryPersistState        string
	querySetRowLocked        string
}

func newStreamProjectionStorage(
	projectionName,
	projectionTable string,
	projectionStateEncoder driverSQL.ProjectionStateEncoder,
	logger goengine.Logger,
) *streamProjectionStorage {
	if logger == nil {
		logger = goengine.NopLogger
	}
	if projectionStateEncoder == nil {
		projectionStateEncoder = defaultProjectionStateEncoder
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)

	return &streamProjectionStorage{
		projectionName:         projectionName,
		projectionStateEncoder: projectionStateEncoder,
		logger:                 logger,

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
			`UPDATE %[1]s SET position = $1, state = $2 WHERE name = $3`,
			projectionTableQuoted,
		),
		querySetRowLocked: fmt.Sprintf(
			`UPDATE ONLY %[1]s SET locked = $2 WHERE name = $1`,
			projectionTableQuoted,
		),
	}
}

func (s *streamProjectionStorage) PersistState(conn *sql.Conn, notification *driverSQL.ProjectionNotification, state driverSQL.ProjectionState) error {
	encodedState, err := s.projectionStateEncoder(state.ProjectionState)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), s.queryPersistState, state.Position, encodedState, s.projectionName)
	if err != nil {
		return err
	}
	s.logger.
		WithFields(goengine.Fields{
			"notification": notification,
			"state":        state,
		}).
		Debug("updated projection state")

	return nil
}

func (s *streamProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (func(), *driverSQL.ProjectionRawState, error) {
	logger := s.logger.WithField("notification", notification)

	var res *sql.Row
	if notification == nil {
		res = conn.QueryRowContext(ctx, s.queryAcquireLock, s.projectionName)
	} else {
		res = conn.QueryRowContext(ctx, s.queryAcquirePositionLock, s.projectionName, notification.No)
	}

	var (
		acquiredLock bool
		locked       bool
		rawState     []byte
		position     int64
	)
	if err := res.Scan(&acquiredLock, &locked, &position, &rawState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, nil, driverSQL.ErrNoProjectionRequired
		}

		return nil, nil, err
	}

	if !acquiredLock {
		return nil, nil, driverSQL.ErrProjectionFailedToLock
	}

	if locked {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := s.releaseProjectionConnectionLock(conn); err != nil {
			logger.WithError(err).Error("failed to release lock for a projection with a locked row")
		} else {
			logger.Debug("released connection lock for a locked projection")
		}

		return nil, nil, driverSQL.ErrProjectionPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(ctx, s.querySetRowLocked, s.projectionName, true)
	if err != nil {
		if err := s.releaseProjectionLock(conn); err != nil {
			logger.WithError(err).Error("failed to release lock while setting projection row as locked")
		} else {
			logger.Debug("failed to set projection as locked")
		}

		return nil, nil, err
	}

	logger.Debug("acquired projection lock")

	return func() {
		if err := s.releaseProjectionLock(conn); err != nil {
			logger.WithError(err).Error("failed to release projection")
		} else {
			logger.Debug("released projection lock")
		}
	}, &driverSQL.ProjectionRawState{Position: position, ProjectionState: rawState}, nil
}

func (s *streamProjectionStorage) releaseProjectionLock(conn *sql.Conn) error {
	// Set the projection as row unlocked
	_, err := conn.ExecContext(context.Background(), s.querySetRowLocked, s.projectionName, false)
	if err != nil {
		return err
	}

	return s.releaseProjectionConnectionLock(conn)
}

func (s *streamProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn) error {
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
