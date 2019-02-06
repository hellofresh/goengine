package goengine

// InvalidArgumentError indicates that the caller is in error and passed an incorrect value.
type InvalidArgumentError string

func (i InvalidArgumentError) Error() string {
	return "goengine: invalid argument: " + string(i)
}
