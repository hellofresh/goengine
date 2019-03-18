package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
)

func StreamProjectionEventStreamLoader(eventStore driverSQL.ReadOnlyEventStore, streamName goengine.StreamName) driverSQL.EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *driverSQL.ProjectionNotification, position int64) (goengine.EventStream, error) {
		return eventStore.LoadWithConnection(ctx, conn, streamName, position+1, nil, nil)
	}
}

var _ driverSQL.ProjectionStorage = &StreamProjectionStorage{}

type StreamProjectionStorage struct {
	projectionName         string
	projectionStateEncoder driverSQL.ProjectionStateEncoder

	logger goengine.Logger

	queryCreateProjection    string
	queryAcquireLock         string
	queryAcquirePositionLock string
	queryReleaseLock         string
	queryPersistState        string
	querySetRowLocked        string
}

func NewStreamProjectionStorage(
	projectionName,
	projectionTable string,
	projectionStateEncoder driverSQL.ProjectionStateEncoder,
	logger goengine.Logger,
) (*StreamProjectionStorage, error) {
	switch {
	case strings.TrimSpace(projectionName) == "":
		return nil, goengine.InvalidArgumentError("projectionName")
	case strings.TrimSpace(projectionTable) == "":
		return nil, goengine.InvalidArgumentError("projectionTable")
	}

	if logger == nil {
		logger = goengine.NopLogger
	}
	if projectionStateEncoder == nil {
		projectionStateEncoder = defaultProjectionStateEncoder
	}

	projectionTableQuoted := QuoteIdentifier(projectionTable)
	projectionTableStr := QuoteString(projectionTable)

	/* #nosec G201 */
	return &StreamProjectionStorage{
		projectionName:         projectionName,
		projectionStateEncoder: projectionStateEncoder,
		logger:                 logger,

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
			`UPDATE %[1]s SET position = $1, state = $2 WHERE name = $3`,
			projectionTableQuoted,
		),
		querySetRowLocked: fmt.Sprintf(
			`UPDATE ONLY %[1]s SET locked = $2 WHERE name = $1`,
			projectionTableQuoted,
		),
	}, nil
}

func (s *StreamProjectionStorage) CreateProjection(ctx context.Context, conn driverSQL.Execer) error {
	_, err := conn.ExecContext(ctx, s.queryCreateProjection, s.projectionName)
	return err
}

func (s *StreamProjectionStorage) PersistState(conn driverSQL.Execer, notification *driverSQL.ProjectionNotification, state driverSQL.ProjectionState) error {
	encodedState, err := s.projectionStateEncoder(state.ProjectionState)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(context.Background(), s.queryPersistState, state.Position, encodedState, s.projectionName)
	if err != nil {
		return err
	}
	s.logger.Debug("updated projection state", func(e goengine.LoggerEntry) {
		if notification == nil {
			e.Any("notification", nil)
		} else {
			e.Int64("notification.no", notification.No)
			e.String("notification.aggregate_id", notification.AggregateID)
		}
		e.Any("state", state)
	})

	return nil
}

func (s *StreamProjectionStorage) Acquire(
	ctx context.Context,
	conn *sql.Conn,
	notification *driverSQL.ProjectionNotification,
) (func(), *driverSQL.ProjectionRawState, error) {
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
		acquiredLock    bool
		locked          bool
		projectionState driverSQL.ProjectionRawState
	)
	if err := res.Scan(&acquiredLock, &locked, &projectionState.Position, &projectionState.ProjectionState); err != nil {
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
			s.logger.Error("failed to release lock for a projection with a locked row", func(e goengine.LoggerEntry) {
				logFields(e)
				e.Error(err)
			})
		} else {
			s.logger.Debug("released connection lock for a locked projection", logFields)
		}

		return nil, nil, driverSQL.ErrProjectionPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(ctx, s.querySetRowLocked, s.projectionName, true)
	if err != nil {
		if releaseErr := s.releaseProjectionLock(conn); releaseErr != nil {
			s.logger.Error("failed to release lock while setting projection row as locked", func(e goengine.LoggerEntry) {
				logFields(e)
				e.Error(releaseErr)
			})
		} else {
			s.logger.Debug("failed to set projection as locked", logFields)
		}

		return nil, nil, err
	}

	s.logger.Debug("acquired projection lock", logFields)

	return func() {
		if err := s.releaseProjectionLock(conn); err != nil {
			s.logger.Error("failed to release projection", func(e goengine.LoggerEntry) {
				logFields(e)
				e.Error(err)
			})
		} else {
			s.logger.Debug("released projection lock", logFields)
		}
	}, &projectionState, nil
}

func (s *StreamProjectionStorage) releaseProjectionLock(conn *sql.Conn) error {
	// Set the projection as row unlocked
	_, err := conn.ExecContext(context.Background(), s.querySetRowLocked, s.projectionName, false)
	if err != nil {
		return err
	}

	return s.releaseProjectionConnectionLock(conn)
}

func (s *StreamProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn) error {
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
