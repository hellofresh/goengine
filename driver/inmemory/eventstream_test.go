//go:build unit
// +build unit

package inmemory_test

import (
	"testing"

	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/driver/inmemory"
	"github.com/hellofresh/goengine/v2/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventStream(t *testing.T) {
	type streamTestCases struct {
		title          string
		messages       []goengine.Message
		messageNumbers []int64
	}

	testCases := []streamTestCases{
		{
			"Stream with messages",
			[]goengine.Message{
				&mocks.Message{},
				&mocks.Message{},
			},
			[]int64{1, 2},
		},
		{
			"Stream with nil message",
			[]goengine.Message{nil},
			[]int64{1},
		},
		{
			"Stream with no messages",
			nil,
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			stream, err := inmemory.NewEventStream(testCase.messages, testCase.messageNumbers)

			asserts := assert.New(t)
			asserts.NoError(err)
			asserts.NotNil(stream)

			messages, messageNumbers, err := goengine.ReadEventStream(stream)
			asserts.NoError(err)
			asserts.NoError(stream.Err(), "no exception was expected while reading the stream")

			asserts.Equal(testCase.messages, messages)
			asserts.Equal(testCase.messageNumbers, messageNumbers)

			// Message should return an error when no messages are left
			msg, msgNumber, err := stream.Message()
			asserts.Nil(msg)
			asserts.Empty(msgNumber)
			asserts.Error(err)

			// Next should return false after the loop
			asserts.False(stream.Next())
		})
	}

	t.Run("invalid arguments", func(t *testing.T) {
		stream, err := inmemory.NewEventStream(nil, []int64{1})

		assert.Equal(t, err, inmemory.ErrMessageNumberCountMismatch)
		assert.Nil(t, stream)
	})

	t.Run("iteration must start before a message can be fetched", func(t *testing.T) {
		stream, err := inmemory.NewEventStream([]goengine.Message{}, []int64{})
		require.NoError(t, err)

		msg, msgNumber, err := stream.Message()

		asserts := assert.New(t)
		asserts.Equal(inmemory.ErrEventStreamNotStarted, err)
		asserts.Nil(msg)
		asserts.Empty(msgNumber)
	})
}
