package sql

type (
	// Metrics a structured metrics interface
	Metrics interface {
		ReceivedNotification(isNotification bool)
		QueueNotification(notification *ProjectionNotification)
		StartNotificationProcessing(notification *ProjectionNotification)
		FinishNotificationProcessing(notification *ProjectionNotification, success bool, retry bool)
	}
)
