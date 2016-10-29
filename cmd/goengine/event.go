package main

import "time"

type SomethingHappened struct {
	Ocurred time.Time `json:"ocurred_on"`
}

func NewSomethingHappened() *SomethingHappened {
	return &SomethingHappened{time.Now()}
}

func (e *SomethingHappened) OcurredOn() time.Time {
	return e.Ocurred
}
