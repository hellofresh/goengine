// +build unit

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamProjectionStorage_PersistState(t *testing.T) {
	mockedProjectionState := "I'm a projection state"
	notification := &driverSQL.ProjectionNotification{
		No:          1,
		AggregateID: "d8490e85-dd22-4c32-9cb0-a29e57949951",
	}
	projectionState := driverSQL.ProjectionState{
		Position:        5,
		ProjectionState: mockedProjectionState,
	}

	test.RunWithMockDB(t, "Persist projection state", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		stateEncoderCalls := 0
		stateEncoder := func(i interface{}) (bytes []byte, e error) {
			assert.Equal(t, mockedProjectionState, i)

			stateEncoderCalls++
			return []byte(mockedProjectionState), nil
		}

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		storage, err := newStreamProjectionStorage("my_projection", "projection_table", stateEncoder, goengine.NopLogger)
		require.NoError(t, err)

		dbMock.ExpectExec("^UPDATE \"projection_table\" SET").
			WithArgs(5, []byte(mockedProjectionState), "my_projection").
			WillReturnResult(sqlmock.NewResult(18, 1))

		err = storage.PersistState(conn, notification, projectionState)
		assert.NoError(t, err)
		assert.Equal(t, 1, stateEncoderCalls)
	})

	test.RunWithMockDB(t, "Persist projection state (using default encoder)", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		projectionState := driverSQL.ProjectionState{
			Position:        6,
			ProjectionState: nil,
		}

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		storage, err := newStreamProjectionStorage("my_projection", "projection_table", nil, goengine.NopLogger)
		require.NoError(t, err)

		dbMock.ExpectExec("^UPDATE \"projection_table\" SET").
			WithArgs(6, []byte("{}"), "my_projection").
			WillReturnResult(sqlmock.NewResult(18, 1))

		err = storage.PersistState(conn, notification, projectionState)
		assert.NoError(t, err)
	})

	test.RunWithMockDB(t, "Fail when state encoding fails", func(t *testing.T, db *sql.DB, dbMock sqlmock.Sqlmock) {
		stateEncoderErr := errors.New("Failed to encode")
		stateEncoder := func(i interface{}) (bytes []byte, e error) {
			return nil, stateEncoderErr
		}

		conn, err := db.Conn(context.Background())
		require.NoError(t, err)

		storage, err := newStreamProjectionStorage("my_projection", "projection_table", stateEncoder, goengine.NopLogger)
		require.NoError(t, err)

		err = storage.PersistState(conn, notification, projectionState)
		assert.Equal(t, stateEncoderErr, err)
	})

}
