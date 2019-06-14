package sql

type (
	// Metrics a structured metrics interface
	Metrics interface {
		// ReceivedNotification sends the metric to keep count of notifications received by goengine
		ReceivedNotification(isNotification bool)
		// QueueNotification is called when a notification is queued.
		// It saves start time for an event on aggregate when it's queued
		QueueNotification(notification *ProjectionNotification)
		// StartNotificationProcessing is called when a notification processing is started
		// It saves start time for an event on aggregate when it's picked to be processed by background processor
		StartNotificationProcessing(notification *ProjectionNotification)
		// FinishNotificationProcessing is called when a notification processing is finished
		// It actually sends metrics calculating duration for which a notification spends in queue and then processed by background processor
		FinishNotificationProcessing(notification *ProjectionNotification, success bool)
	}
)
