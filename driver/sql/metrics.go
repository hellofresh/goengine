package sql

type (
	// Metrics a structured metrics interface
	Metrics interface {
		ReceivedNotification(isNotification bool)
		QueueNotification(notification *ProjectionNotification) bool
		StartNotificationProcessing(notification *ProjectionNotification) bool
		FinishNotificationProcessing(notification *ProjectionNotification, success bool) bool
	}
)
