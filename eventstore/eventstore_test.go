// +build unit

package eventstore_test

import (
	"errors"
	"testing"

	goengine_dev "github.com/hellofresh/goengine-dev"
	"github.com/hellofresh/goengine/eventstore"
	"github.com/hellofresh/goengine/mocks"
	"github.com/stretchr/testify/assert"
)

func TestReadEventStream(t *testing.T) {
	mockedMessages := make([]goengine_dev.Message, 4)
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
		stream.On("Message").Return(
			func() goengine_dev.Message {
				return mockedMessages[streamIndex]
			},
			func() int64 {
				return int64(streamIndex + 1)
			},
			nil,
		)
		stream.On("Err").Return(nil)

		messages, numbers, err := eventstore.ReadEventStream(stream)

		if assert.NoError(t, err) {
			assert.Equal(t, mockedMessages, messages)
			assert.Equal(t, []int64{1, 2, 3, 4}, numbers)
		}
	})

	t.Run("Error while iterating", func(t *testing.T) {
		expectedError := errors.New("I failed")

		stream := &mocks.EventStream{}
		stream.On("Next").Return(func() bool {
			return false
		})
		stream.On("Err").Return(expectedError)

		messages, numbers, err := eventstore.ReadEventStream(stream)

		asserts := assert.New(t)
		if asserts.Error(err) {
			asserts.Equal(expectedError, err)
		}
		asserts.Empty(messages)
		asserts.Empty(numbers)
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
			func() goengine_dev.Message {
				return mockedMessages[streamIndex]
			},
			func() int64 {
				return int64(streamIndex + 1)
			},
			func() error {
				if streamIndex == 2 {
					return expectedError
				}

				return nil
			},
		)
		stream.On("Err").Return(nil)

		messages, numbers, err := eventstore.ReadEventStream(stream)

		asserts := assert.New(t)
		if asserts.Error(err) {
			asserts.Equal(expectedError, err)
		}
		asserts.Empty(numbers)
		asserts.Empty(messages)
	})
}
