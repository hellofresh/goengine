package errors

import "errors"

var (
	ErrorTypeNotRegistred = errors.New("The type is not registereds")
	ErrorTypeNotStruct    = errors.New("Input param is not a struct")
	ErrorTypeNotFound     = errors.New("The type was not found")
)
