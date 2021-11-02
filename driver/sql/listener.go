package sql

import "context"

// Listener listens to an event stream and triggers a notification when an event was appended
type Listener interface {
	// Listen starts listening to the event stream and call the trigger when an event was appended
	Listen(ctx context.Context, trigger ProjectionTrigger) error
}
