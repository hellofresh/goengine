package goengine

type (
	// MessagePayloadConverter an interface describing converting payload data
	MessagePayloadConverter interface {
		// ConvertPayload generates unique name for the event_name
		ConvertPayload(payload interface{}) (name string, data []byte, err error)
	}

	// MessagePayloadFactory is used to reconstruct message payloads
	MessagePayloadFactory interface {
		// CreatePayload returns a reconstructed payload or an error
		CreatePayload(payloadType string, data interface{}) (interface{}, error)
	}

	// MessagePayloadResolver is used resolve the event_name of a payload
	MessagePayloadResolver interface {
		// ResolveName resolves the name of the underlying payload type
		ResolveName(payload interface{}) (string, error)
	}
)
