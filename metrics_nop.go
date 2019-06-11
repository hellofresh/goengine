package goengine

var NopMetrics Metrics = &nopMetrics{}

type nopMetrics struct{}

func (nm *nopMetrics) ReceivedNotification(isNotification bool)                             {}
func (nm *nopMetrics) QueueNotification(notification interface{})           {}
func (nm *nopMetrics) StartNotificationProcessing(notification interface{}) {}
func (nm *nopMetrics) FinishNotificationProcessing(notification interface{}, success bool, retry bool) {
}
