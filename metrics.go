package goengine

type (
	// Metrics a structured metrics interface
	Metrics interface {
		ReceivedNotification(isNotification bool)
		QueueNotification(notification interface{})
		StartNotificationProcessing(notification interface{})
		FinishNotificationProcessing(notification interface{}, success bool, retry bool)
	}
)
