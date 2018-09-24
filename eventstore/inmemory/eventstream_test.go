// +build unit

package inmemory_test

import (
	"testing"

	"github.com/hellofresh/goengine/eventstore"

	"github.com/hellofresh/goengine/eventstore/inmemory"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestEventStream(t *testing.T) {
	type streamTestCases struct {
		title          string
		messages       []messaging.Message
		messageNumbers []int64
	}

	testCases := []streamTestCases{
		{
			"Stream with messages",
			[]messaging.Message{
				&mocks.Message{},
				&mocks.Message{},
			},
			[]int64{1, 2},
		},
		{
			"Stream with nil message",
			[]messaging.Message{nil},
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
			asserts := assert.New(t)

			stream, err := inmemory.NewEventStream(testCase.messages, testCase.messageNumbers)
			if !asserts.NoError(err) {
				return
			}
			asserts.NotNil(stream)

			messages, messageNumbers, err := eventstore.ReadEventStream(stream)
			if !asserts.NoError(stream.Err()) {
				asserts.FailNow("no exception was expected while reading the stream")
			}

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
		asserts := assert.New(t)

		stream, err := inmemory.NewEventStream(nil, []int64{1})
		if asserts.Error(err) {
			asserts.Equal(err, inmemory.ErrMessageNumberCountMismatch)
		}
		asserts.Nil(stream)
	})
}
