package eventstore_test

import (
	"errors"
	"testing"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/messaging"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestReadEventStream(t *testing.T) {
	mockedMessages := make([]messaging.Message, 4)
	for i := range mockedMessages {
		m := &mocks.Message{}
		m.On("Payload").Return(i)
		mockedMessages[i] = m
	}

	t.Run("Get the message of the stream", func(t *testing.T) {
		stream := &mocks.EventStream{}
		streamIndex := -1
		stream.On("Next").Return(func() bool {
			streamIndex++
			return streamIndex < 4
		})
		stream.On("Message").Return(func() messaging.Message {
			return mockedMessages[streamIndex]
		}, nil)
		stream.On("Err").Return(nil)

		messages, err := eventstore.ReadEventStream(stream)

		if assert.NoError(t, err) {
			assert.Equal(t, mockedMessages, messages)
		}
	})

	t.Run("Error while iterating", func(t *testing.T) {
		expectedError := errors.New("I failed")

		stream := &mocks.EventStream{}
		stream.On("Next").Return(func() bool {
			return false
		})
		stream.On("Err").Return(expectedError)

		messages, err := eventstore.ReadEventStream(stream)

		asserts := assert.New(t)
		if asserts.Error(err) {
			asserts.Equal(expectedError, err)
		}
		asserts.Empty(messages)
	})

	t.Run("Error while fetching a Message", func(t *testing.T) {
		expectedError := errors.New("I failed")

		stream := &mocks.EventStream{}
		streamIndex := -1
		stream.On("Next").Return(func() bool {
			streamIndex++
			return streamIndex < 4
		})
		stream.On("Message").Return(
			func() messaging.Message {
				return mockedMessages[streamIndex]
			},
			func() error {
				if streamIndex == 2 {
					return expectedError
				}

				return nil
			},
		)
		stream.On("Err").Return(nil)

		messages, err := eventstore.ReadEventStream(stream)

		asserts := assert.New(t)
		if asserts.Error(err) {
			asserts.Equal(expectedError, err)
		}
		asserts.Empty(messages)
	})
}
