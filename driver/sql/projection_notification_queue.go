package sql

import (
	"context"
)

// NotificationQueuer describes a smart queue for projection notifications
type NotificationQueuer interface {
	Open() chan struct{}
	Close()

	Empty() bool
	Next(context.Context) (*ProjectionNotification, bool)

	Queue(context.Context, *ProjectionNotification) error
	ReQueue(context.Context, *ProjectionNotification) error
}
