package eventstore

import (
	goengine_dev "github.com/hellofresh/goengine-dev"
)

// ReadEventStream reads the entire event stream and returns it's content as a slice.
// The main purpose of the function is for testing and debugging.
func ReadEventStream(stream goengine_dev.EventStream) ([]goengine_dev.Message, []int64, error) {
	var messages []goengine_dev.Message
	var messageNumbers []int64
	for stream.Next() {
		msg, msgNumber, err := stream.Message()
		if err != nil {
			return nil, nil, err
		}

		messages = append(messages, msg)
		messageNumbers = append(messageNumbers, msgNumber)
	}

	if err := stream.Err(); err != nil {
		return nil, nil, err
	}

	return messages, messageNumbers, nil
}
