// +build unit

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql/postgres"
	"github.com/hellofresh/goengine/mocks/driver/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextTestKey string

func TestAdvisoryLockStreamProjectionStorage_CreateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage, err := postgres.NewAdvisoryLockStreamProjectionStorage(
		"my_projection",
		"event_store_projection",
		sql.NewProjectionStateSerialization(ctrl),
		true,
		goengine.NopLogger,
	)
	require.NoError(t, err)

	// Test no error
	ctx := context.WithValue(context.Background(), contextTestKey("no error"), true)
	mockDB := sql.NewExecer(ctrl)
	mockDB.EXPECT().ExecContext(ctx, gomock.AssignableToTypeOf(""), "my_projection").Times(1).Return(nil, nil)

	err = storage.CreateProjection(ctx, mockDB)
	assert.NoError(t, err)

	// Test DB error
	expectedErr := errors.New("test error")

	ctx = context.WithValue(context.Background(), contextTestKey("no error"), false)
	mockDB = sql.NewExecer(ctrl)
	mockDB.EXPECT().ExecContext(ctx, gomock.AssignableToTypeOf(""), "my_projection").Times(1).Return(nil, expectedErr)

	err = storage.CreateProjection(ctx, mockDB)
	assert.Equal(t, expectedErr, err)
}
