// +build unit

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine"
	driverSQL "github.com/hellofresh/goengine/driver/sql"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/mocks/driver/sql"
	"github.com/stretchr/testify/require"
)

func TestAdvisoryLockAggregateProjectionStorage_PersistFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	notification := &driverSQL.ProjectionNotification{
		No:          1,
		AggregateID: "20a151cc-e44e-4133-9491-8dc341032d37",
	}

	storage, err := postgres.NewAdvisoryLockAggregateProjectionStorage(
		"event_store_table",
		"event_store_projection_table",
		sql.NewProjectionStateSerialization(ctrl),
		true,
		goengine.NopLogger,
	)
	require.NoError(t, err)

	// No error
	mockDB := sql.NewExecer(ctrl)
	mockDB.EXPECT().
		ExecContext(context.Background(), gomock.AssignableToTypeOf(""), "20a151cc-e44e-4133-9491-8dc341032d37").
		Return(nil, nil).
		Times(1)

	err = storage.PersistFailure(mockDB, notification)
	assert.NoError(t, err)

	// DB error
	expectedErr := errors.New("test error")

	mockDB = sql.NewExecer(ctrl)
	mockDB.EXPECT().
		ExecContext(context.Background(), gomock.AssignableToTypeOf(""), "20a151cc-e44e-4133-9491-8dc341032d37").
		Return(nil, expectedErr).
		Times(1)

	err = storage.PersistFailure(mockDB, notification)
	assert.Equal(t, expectedErr, err)
}
