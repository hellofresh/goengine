package goengine

import (
	"github.com/hellofresh/goengine/driver/sql"
)

type (
	// Metrics a structured metrics interface
	Metrics interface {
		ReceivedNotification(isNotification bool)
		QueueNotification(notification *sql.ProjectionNotification)
		StartNotificationProcessing(notification *sql.ProjectionNotification)
		FinishNotificationProcessing(notification *sql.ProjectionNotification)
	}
)
