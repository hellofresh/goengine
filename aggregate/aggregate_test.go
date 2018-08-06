package aggregate_test

import (
	"testing"
	"time"

	"github.com/hellofresh/goengine/aggregate"
	"github.com/stretchr/testify/assert"
)

func TestCurrentTime(t *testing.T) {
	asserts := assert.New(t)

	currentTime := aggregate.CurrentTime()
	asserts.Equal(time.UTC, currentTime.Location(), "Expected time to return a UTC time")

	time.Sleep(time.Nanosecond)
	secondTime := aggregate.CurrentTime()
	asserts.Equal(time.UTC, secondTime.Location(), "Expected secondTime to return a UTC time")

	asserts.NotEqual(currentTime, secondTime, "Expected CurrentTime() to return a new time")
}

func TestGenerateID(t *testing.T) {
	asserts := assert.New(t)

	firstID := aggregate.GenerateID()
	asserts.NotEmpty(firstID, "A aggregate.ID should not be empty")

	secondID := aggregate.GenerateID()
	asserts.NotEmpty(secondID, "A aggregate.ID should not be empty")

	asserts.NotEqual(firstID, secondID, "Expected GenerateID() to return a different ID")
}
