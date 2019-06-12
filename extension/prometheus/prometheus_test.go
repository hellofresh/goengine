// +build unit

package prometheus

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hellofresh/goengine/driver/sql"
)

func TestPrometheusMetrics(t *testing.T) {
	metrics := NewMetrics()

	testSQLProjection := sql.ProjectionNotification{
		No:          1,
		AggregateID: "C56A4180-65AA-42EC-A945-5FD21DEC0538",
	}

	metrics.QueueNotification(&testSQLProjection)
	metrics.StartNotificationProcessing(&testSQLProjection)

	memAddress := fmt.Sprintf("%p", &testSQLProjection)
	queueStartTime, ok := metrics.notificationStartTimes.Load("q" + memAddress)
	assert.True(t, ok)
	assert.IsType(t, time.Time{}, queueStartTime)

	processingStartTime, _ := metrics.notificationStartTimes.Load("p" + memAddress)
	assert.True(t, ok)
	assert.IsType(t, time.Time{}, processingStartTime)
}
