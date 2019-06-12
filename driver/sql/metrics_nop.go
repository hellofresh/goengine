package sql

// NopMetrics is default Metrics handler in case nil is passed
var NopMetrics Metrics = &nopMetrics{}

type nopMetrics struct{}

func (nm *nopMetrics) ReceivedNotification(isNotification bool) {}
func (nm *nopMetrics) QueueNotification(notification *ProjectionNotification) {
}
func (nm *nopMetrics) StartNotificationProcessing(notification *ProjectionNotification) {

}
func (nm *nopMetrics) FinishNotificationProcessing(notification *ProjectionNotification, success bool) {
}
