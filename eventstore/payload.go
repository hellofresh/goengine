package eventstore

type (
	// PayloadConverter an interface describing converting payload data
	PayloadConverter interface {
		// ConvertPayload generates unique name for the event_name
		ConvertPayload(payload interface{}) (name string, data []byte, err error)
	}

	// PayloadFactory is used to reconstruct message payloads
	PayloadFactory interface {
		// CreatePayload returns a reconstructed payload or a error
		CreatePayload(payloadType string, data interface{}) (interface{}, error)
	}
)
