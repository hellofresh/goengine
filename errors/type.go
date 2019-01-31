package errors

import "errors"

var (
	// ErrorTypeNotRegistered ...
	ErrorTypeNotRegistered = errors.New("the type is not registered")
	// ErrorTypeNotStruct ...
	ErrorTypeNotStruct = errors.New("input param is not a struct")
	// ErrorTypeNotFound ...
	ErrorTypeNotFound = errors.New("the type was not found")
)
