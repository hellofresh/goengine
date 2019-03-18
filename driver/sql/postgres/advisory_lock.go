package postgres

import (
	"context"
	"database/sql"
	"errors"

	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
)

var _ driverSQL.ProjectorTransaction = &advisoryLockProjectorTransaction{}

type advisoryLockProjectorTransaction struct {
	conn              *sql.Conn
	queryPersistState string
	queryReleaseLock  string

	stateSerialization driverSQL.ProjectionStateSerialization
	rawState           *driverSQL.ProjectionRawState

	projectionID    string
	projectionState driverSQL.ProjectionState

	logger goengine.Logger
}

func (t *advisoryLockProjectorTransaction) AcquireState(ctx context.Context) (driverSQL.ProjectionState, error) {
	if t.rawState == nil {
		return t.projectionState, nil
	}

	var err error
	state := driverSQL.ProjectionState{
		Position: t.rawState.Position,
	}

	// Decode or initialize projection state
	if state.Position == 0 {
		// This is the fist time the projection runs so initialize the state
		state.ProjectionState, err = t.stateSerialization.Init(ctx)
	} else {
		// Unmarshal the projection state
		state.ProjectionState, err = t.stateSerialization.DecodeState(t.rawState.ProjectionState)
	}

	if err != nil {
		return state, err
	}

	t.projectionState = state
	t.rawState = nil

	return t.projectionState, err
}

func (t *advisoryLockProjectorTransaction) CommitState(newState driverSQL.ProjectionState) error {
	encodedState, err := t.stateSerialization.EncodeState(newState.ProjectionState)
	if err != nil {
		return err
	}

	_, err = t.conn.ExecContext(context.Background(), t.queryPersistState, t.projectionID, newState.Position, encodedState)
	if err != nil {
		return err
	}

	t.projectionState = newState

	t.logger.Debug("updated projection state", func(e goengine.LoggerEntry) {
		e.String("projection_id", t.projectionID)
		e.Int64("projection_position", newState.Position)
		e.Any("state", newState)
	})

	return nil
}

func (t *advisoryLockProjectorTransaction) Close() error {
	res := t.conn.QueryRowContext(context.Background(), t.queryReleaseLock, t.projectionID)

	var unlocked bool
	if err := res.Scan(&unlocked); err != nil {
		return err
	}

	if !unlocked {
		return errors.New("failed to release db connection projection lock")
	}

	t.logger.Debug("released projection lock", func(e goengine.LoggerEntry) {
		e.String("projection_id", t.projectionID)
	})

	return nil
}

type advisoryLockWithUpdateProjectorTransaction struct {
	advisoryLockProjectorTransaction

	querySetRowLocked string
}

func (t *advisoryLockWithUpdateProjectorTransaction) AcquireState(ctx context.Context) (driverSQL.ProjectionState, error) {
	if t.rawState == nil {
		return t.projectionState, nil
	}

	// Set the projection as row locked
	_, err := t.conn.ExecContext(ctx, t.querySetRowLocked, t.projectionID, true)
	if err != nil {
		return driverSQL.ProjectionState{
			Position: t.rawState.Position,
		}, err
	}

	return t.advisoryLockProjectorTransaction.AcquireState(ctx)
}

func (t *advisoryLockWithUpdateProjectorTransaction) Close() error {
	// Set the projection as row unlocked
	_, err := t.conn.ExecContext(context.Background(), t.querySetRowLocked, t.projectionID, false)
	if err != nil {
		return err
	}

	return t.advisoryLockProjectorTransaction.Close()
}
