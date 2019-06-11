package goengine

import "github.com/hellofresh/goengine/driver/sql"

var NopMetrics Metrics = &nopMetrics{}

type nopMetrics struct{}

func (nm *nopMetrics) ReceivedNotification(isNotification bool)                              {}
func (nm *nopMetrics) QueueNotification(notification *sql.ProjectionNotification)            {}
func (nm *nopMetrics) StartNotificationProcessing(notification *sql.ProjectionNotification)  {}
func (nm *nopMetrics) FinishNotificationProcessing(notification *sql.ProjectionNotification) {}
