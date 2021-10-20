package sql

import "context"

// Listener listens to an event stream and triggers a notification when a event was appended
type Listener interface {
	// Listen starts listening to the event stream and call the trigger when a event was appended
	Listen(ctx context.Context, trigger ProjectionTrigger) error
}
