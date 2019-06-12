// +build unit

package prometheus_test

import (
	"testing"

	"github.com/hellofresh/goengine/extension/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/driver/sql"
)

func TestPrometheusMetrics(t *testing.T) {
	metrics := prometheus.NewMetrics()

	testSQLProjection := sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	ok := metrics.FinishNotificationProcessing(&testSQLProjection, true)
	assert.False(t, ok)

	ok = metrics.QueueNotification(&testSQLProjection)
	assert.True(t, ok)

	ok = metrics.QueueNotification(&testSQLProjection)
	assert.False(t, ok)

	ok = metrics.FinishNotificationProcessing(&testSQLProjection, true)
	assert.False(t, ok)

	ok = metrics.StartNotificationProcessing(&testSQLProjection)
	assert.True(t, ok)

	ok = metrics.StartNotificationProcessing(&testSQLProjection)
	assert.False(t, ok)

	ok = metrics.FinishNotificationProcessing(&testSQLProjection, true)
	assert.True(t, ok)

}
