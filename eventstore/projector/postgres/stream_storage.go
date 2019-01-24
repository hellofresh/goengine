package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/eventstore/projector"
	"github.com/hellofresh/goengine/eventstore/projector/internal"
	eventStoreSQL "github.com/hellofresh/goengine/eventstore/sql"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

func streamProjectionEventStreamLoader(eventStore eventStoreSQL.ReadOnlyEventStore, streamName eventstore.StreamName) internal.EventStreamLoader {
	return func(ctx context.Context, conn *sql.Conn, notification *projector.Notification, state internal.State) (eventstore.EventStream, error) {
		return eventStore.LoadWithConnection(ctx, conn, streamName, state.Position+1, nil, nil)
	}
}

var _ internal.Storage = &streamProjectionStorage{}

type streamProjectionStorage struct {
	projectionTable string
	projectionName  string

	logger logrus.FieldLogger
}

func (s *streamProjectionStorage) PersistState(conn *sql.Conn, notification *projector.Notification, state internal.State) error {
	jsonState, err := json.Marshal(state.ProjectionState)
	if err != nil {
		return err
	}

	_, err = conn.ExecContext(
		context.Background(),
		fmt.Sprintf(
			`UPDATE %s SET position = $1, state = $2 WHERE name = $3`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		state.Position,
		jsonState,
		s.projectionName,
	)
	if err != nil {
		return err
	}
	s.logger.WithFields(logrus.Fields{
		"state":        state,
		"notification": notification,
	}).Debug("updated projection state")

	return nil
}

func (s *streamProjectionStorage) Acquire(ctx context.Context, conn *sql.Conn, notification *projector.Notification) (func(), *internal.AcquiredState, error) {
	logger := s.logger.WithField("notification", notification)

	var res *sql.Row
	if notification == nil {
		res = conn.QueryRowContext(
			ctx,
			fmt.Sprintf(
				`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), locked, position, state FROM %s WHERE name = $1`,
				quoteString(s.projectionTable),
				pq.QuoteIdentifier(s.projectionTable),
			),
			s.projectionName,
		)
	} else {
		res = conn.QueryRowContext(
			ctx,
			fmt.Sprintf(
				`SELECT pg_try_advisory_lock(%s::regclass::oid::int, no), locked, position, state FROM %s WHERE name = $1 AND position < $2`,
				quoteString(s.projectionTable),
				pq.QuoteIdentifier(s.projectionTable),
			),
			s.projectionName,
			notification.No,
		)
	}

	var (
		acquiredLock bool
		locked       bool
		jsonState    []byte
		position     int64
	)
	if err := res.Scan(&acquiredLock, &locked, &position, &jsonState); err != nil {
		// No rows are returned when the projector is already at the notification position
		if err == sql.ErrNoRows {
			return nil, nil, projector.ErrNoProjectionRequired
		}

		return nil, nil, err
	}

	if !acquiredLock {
		return nil, nil, projector.ErrFailedToLock
	}

	if locked {
		// The projection was locked by another process that died and for this reason not unlocked
		// In this case a application needs to decide what to do to avoid invalid projection states
		if err := s.releaseProjectionConnectionLock(conn); err != nil {
			logger.WithError(err).Error("failed to release lock for a projection with a locked row")
		} else {
			logger.Debug("released connection lock for a locked projection")
		}

		return nil, nil, projector.ErrPreviouslyLocked
	}

	// Set the projection as row locked
	_, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE ONLY %[1]s SET locked = TRUE WHERE name = $1`, pq.QuoteIdentifier(s.projectionTable)),
		s.projectionName,
	)
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
	}, &internal.AcquiredState{Position: position, ProjectionState: jsonState}, nil
}

func (s *streamProjectionStorage) releaseProjectionLock(conn *sql.Conn) error {
	// Set the projection as row unlocked
	_, err := conn.ExecContext(
		context.Background(),
		fmt.Sprintf(
			`UPDATE ONLY %[1]s SET locked = FALSE WHERE name = $1`,
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projectionName,
	)
	if err != nil {
		return err
	}

	return s.releaseProjectionConnectionLock(conn)
}

func (s *streamProjectionStorage) releaseProjectionConnectionLock(conn *sql.Conn) error {
	res := conn.QueryRowContext(
		context.Background(),
		fmt.Sprintf(
			`SELECT pg_advisory_unlock(%s::regclass::oid::int, no) FROM %s WHERE name = $1`,
			quoteString(s.projectionTable),
			pq.QuoteIdentifier(s.projectionTable),
		),
		s.projectionName,
	)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release projection connection lock")
	}

	return nil
}
