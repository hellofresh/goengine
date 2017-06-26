package rabbit

import (
	"math"
	"time"

	"github.com/hellofresh/goengine"
)

type function func() error

func exponential(operation function, maxRetries int) error {
	var err error
	var sleepMs int
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}

		if i == 0 {
			sleepMs = 1
		} else {
			sleepMs = int(math.Exp2(float64(i)) * 100)
		}

		sleepTime := time.Duration(sleepMs) * time.Millisecond
		time.Sleep(sleepTime)
		goengine.Log("Retry exponential", map[string]interface{}{"attempt": i, "sleep": sleepTime.String()}, err)
	}

	return err
}
