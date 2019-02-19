package inmemory

import "time"

type SomethingHappened struct {
	occurredOn time.Time
}

func NewSomethingHappened() SomethingHappened {
	return SomethingHappened{time.Now()}
}

func (e SomethingHappened) OccurredOn() time.Time {
	return e.occurredOn
}
