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
		title    string
		messages []messaging.Message
	}

	testCases := []streamTestCases{
		{
			"Stream with messages",
			[]messaging.Message{
				&mocks.Message{},
				&mocks.Message{},
			},
		},
		{
			"Stream with nil message",
			[]messaging.Message{nil},
		},
		{
			"Stream with no messages",
			nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			stream := inmemory.NewEventStream(testCase.messages)

			asserts := assert.New(t)
			asserts.NotNil(stream)

			messages, err := eventstore.ReadEventStream(stream)
			if !asserts.NoError(stream.Err()) {
				asserts.FailNow("no exception was expected while reading the stream")
			}

			asserts.Equal(testCase.messages, messages)

			// Message should return an error when no messages are left
			msg, err := stream.Message()
			asserts.Nil(msg)
			asserts.Error(err)

			// Next should return false after the loop
			asserts.False(stream.Next())
		})
	}
}
