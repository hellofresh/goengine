package inmemory_test

import (
	"time"
)

type SomethingHappened struct {
	ocurredOn time.Time
}

func NewSomethingHappened() SomethingHappened {
	return SomethingHappened{time.Now()}
}

func (e SomethingHappened) OcurredOn() time.Time {
	return e.ocurredOn
}
