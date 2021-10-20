package sql

// NopMetrics is default Metrics handler in case nil is passed
var NopMetrics Metrics = &nopMetrics{}

type nopMetrics struct{}

func (nm *nopMetrics) ReceivedNotification(bool)                                  {}
func (nm *nopMetrics) QueueNotification(*ProjectionNotification)                  {}
func (nm *nopMetrics) StartNotificationProcessing(*ProjectionNotification)        {}
func (nm *nopMetrics) FinishNotificationProcessing(*ProjectionNotification, bool) {}
