package rabbit

import (
	"math"
	"time"

	log "github.com/sirupsen/logrus"
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
		log.WithFields(log.Fields{"attempt": i, "sleep": sleepTime.String()}).Debug("Retry exponential")
	}

	return err
}
