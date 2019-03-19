package postgres

import (
	"context"
	"database/sql"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
)

var _ driverSQL.ProjectorTransaction = &skipLockProjectorTransaction{}

type skipLockProjectorTransaction struct {
	conn                *sql.Conn
	transaction         *sql.Tx
	queryAcquireRowLock string
	queryPersistState   string

	stateMarshaling driverSQL.ProjectionStateSerialization
	rawState        *driverSQL.ProjectionRawState

	projectionID string

	logger goengine.Logger
}

func (t *skipLockProjectorTransaction) AcquireState(ctx context.Context) (state driverSQL.ProjectionState, err error) {
	defer func() {
		if err != nil && t.transaction != nil {
			if rollbackErr := t.transaction.Rollback(); rollbackErr != nil {
				t.logger.Error("acquire state could not rollback transaction", func(e goengine.LoggerEntry) {
					e.Error(rollbackErr)
				})
			}
		}
	}()

	// If there is no active transaction begin one
	var rawState driverSQL.ProjectionRawState
	if t.transaction == nil {
		t.transaction, err = t.conn.BeginTx(context.Background(), nil)
		if err != nil {
			return state, err
		}

		var (
			acquiredLock bool
			failed       bool
		)
		err = t.transaction.QueryRowContext(ctx, t.queryAcquireRowLock, t.projectionID).
			Scan(&acquiredLock, &failed, &rawState.Position, &rawState.ProjectionState)
		if err != nil {
			// No rows are returned when the projector is already at the notification position
			if err == sql.ErrNoRows {
				return state, driverSQL.ErrNoProjectionRequired
			}

			return
		}

		if !acquiredLock {
			err = driverSQL.ErrProjectionFailedToLock
			return
		}

		if failed {
			err = driverSQL.ErrProjectionPreviouslyLocked
			return
		}
	} else {
		rawState = *t.rawState
	}

	// Decode or initialize projection state
	state.Position = rawState.Position
	if state.Position == 0 {
		// This is the fist time the projection runs so initialize the state
		state.ProjectionState, err = t.stateMarshaling.Init(ctx)
	} else {
		// Unmarshal the projection state
		state.ProjectionState, err = t.stateMarshaling.DecodeState(rawState.ProjectionState)
	}

	if err != nil {
		return
	}

	t.rawState = nil

	return state, nil
}

func (t *skipLockProjectorTransaction) CommitState(newState driverSQL.ProjectionState) error {
	encodedState, err := t.stateMarshaling.EncodeState(newState.ProjectionState)
	if err != nil {
		return err
	}

	_, err = t.transaction.ExecContext(context.Background(), t.queryPersistState, t.projectionID, newState.Position, encodedState)
	if err != nil {
		return err
	}

	if err = t.transaction.Commit(); err != nil {
		return err
	}
	t.transaction = nil

	t.logger.Debug("updated projection state", func(e goengine.LoggerEntry) {
		e.String("projection_id", t.projectionID)
		e.Int64("projection_position", newState.Position)
		e.Any("state", newState)
	})

	return nil
}

func (t *skipLockProjectorTransaction) Close() error {
	if t.transaction == nil {
		return nil
	}

	err := t.transaction.Rollback()
	if err == nil || err == sql.ErrTxDone {
		return nil
	}

	return err
}
