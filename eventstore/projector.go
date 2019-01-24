package eventstore

import (
	"context"

	"github.com/hellofresh/goengine/messaging"
)

type (
	// ProjectionHandler is a func that can do state changes based on a message
	ProjectionHandler func(ctx context.Context, state interface{}, message messaging.Message) (interface{}, error)

	// Projection contains the information of a projection
	Projection interface {
		// Name returns the name of the projection
		Name() string

		// FromStream returns the stream this projection is based on
		FromStream() StreamName

		// Init initializes the projections state
		Init(ctx context.Context) (interface{}, error)

		// ReconstituteState reconstitute the projection state based on the provided state data
		ReconstituteState(data []byte) (interface{}, error)

		// Handlers ...
		Handlers() map[string]ProjectionHandler
	}
)
