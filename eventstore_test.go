//go:build unit
// +build unit

package goengine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hellofresh/goengine/v2"
	"github.com/hellofresh/goengine/v2/mocks"
	"github.com/stretchr/testify/assert"
)

func TestReadEventStream(t *testing.T) {
	mockedMessages := make([]goengine.Message, 4)
	for i := range mockedMessages {
		mockedMessages[i] = mocks.NewDummyMessage(goengine.UUID{}, i, nil, time.Now())
	}

	t.Run("Get the message of the stream", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stream := mocks.NewEventStream(ctrl)
		streamIndex := -1
		stream.EXPECT().Next().DoAndReturn(func() bool {
			streamIndex++
			return streamIndex < 4
		}).Times(5)
		stream.EXPECT().Message().DoAndReturn(func() (goengine.Message, int64, error) {
			return mockedMessages[streamIndex], int64(streamIndex + 1), nil
		}).Times(4)
		stream.EXPECT().Err().Return(nil).MinTimes(1)

		messages, numbers, err := goengine.ReadEventStream(stream)

		assert.NoError(t, err)
		assert.Equal(t, mockedMessages, messages)
		assert.Equal(t, []int64{1, 2, 3, 4}, numbers)
	})

	t.Run("Error while iterating", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("I failed")

		stream := mocks.NewEventStream(ctrl)
		stream.EXPECT().Next().Return(false)
		stream.EXPECT().Err().Return(expectedError)

		messages, numbers, err := goengine.ReadEventStream(stream)

		asserts := assert.New(t)
		asserts.Equal(expectedError, err)
		asserts.Empty(messages)
		asserts.Empty(numbers)
	})

	t.Run("Error while fetching a Message", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("I failed")

		stream := mocks.NewEventStream(ctrl)
		streamIndex := -1
		stream.EXPECT().Next().DoAndReturn(func() bool {
			streamIndex++
			return streamIndex < 4
		}).Times(3)
		stream.EXPECT().Message().DoAndReturn(func() (goengine.Message, int64, error) {
			var err error
			if streamIndex == 2 {
				err = expectedError
			}
			return mockedMessages[streamIndex], int64(streamIndex + 1), err
		}).Times(3)

		messages, numbers, err := goengine.ReadEventStream(stream)

		asserts := assert.New(t)
		asserts.Equal(expectedError, err)
		asserts.Empty(numbers)
		asserts.Empty(messages)
	})
}
