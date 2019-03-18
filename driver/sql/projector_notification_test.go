// +build unit

package sql

import (
	"context"
	"testing"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapProjectionHandlerToTrapError(t *testing.T) {
	errorCases := []struct {
		title         string
		handler       goengine.MessageHandler
		expectedCause string
	}{
		{
			"panic with error",
			func(context.Context, interface{}, goengine.Message) (interface{}, error) {
				panic(errors.New("crazy"))
			},
			"crazy",
		},
		{
			"panic with string",
			func(context.Context, interface{}, goengine.Message) (interface{}, error) { panic("evil") },
			"evil",
		},
		{
			"panic with struct",
			func(context.Context, interface{}, goengine.Message) (interface{}, error) {
				panic(struct {
					test string
				}{"manic"})
			},
			"unknown panic: (struct { test string }) {manic}",
		},
		{
			"error return",
			func(context.Context, interface{}, goengine.Message) (interface{}, error) {
				return nil, errors.New("world")
			},
			"world",
		},
	}

	for _, testCase := range errorCases {
		t.Run(testCase.title, func(t *testing.T) {
			wrapped := wrapProjectionHandlerToTrapError(testCase.handler)

			state, err := wrapped(context.Background(), nil, &aggregate.Changed{})
			switch x := err.(type) {
			case *ProjectionHandlerError:
				require.EqualError(t, x.Cause(), testCase.expectedCause)
			default:
				t.Errorf("expected *projector.ProjectionHandlerError error got %T: %s", x, x.Error())
			}
			require.Nil(t, state)
		})
	}

	t.Run("No error occurred", func(t *testing.T) {
		var (
			initialCtx               = context.Background()
			initialState interface{} = "state"
			initialMsg               = &aggregate.Changed{}
		)

		wrapped := wrapProjectionHandlerToTrapError(func(ctx context.Context, state interface{}, message goengine.Message) (interface{}, error) {
			assert.True(t, initialCtx == ctx)
			assert.True(t, initialState == state)
			assert.True(t, initialMsg == message)

			return "yay", nil
		})

		state, err := wrapped(initialCtx, initialState, initialMsg)

		require.Equal(t, "yay", state)
		require.Nil(t, err)
	})
}
