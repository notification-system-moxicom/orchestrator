package errors

import (
	"errors"
	"fmt"
)

type ConfigurationError struct {
	msg string
	err error
}

func NewConfigurationError(msg string, err error) ConfigurationError {
	return ConfigurationError{msg: msg, err: err}
}

func (e ConfigurationError) Error() string {
	if e.err == nil {
		return e.msg
	}

	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e ConfigurationError) Is(err error) bool {
	var configurationError ConfigurationError

	ok := errors.As(err, &configurationError)

	return ok
}

type InvalidMessageError struct {
	msg string
	err error
}

func NewInvalidMessageError(msg string, err error) InvalidMessageError {
	return InvalidMessageError{msg: msg, err: err}
}

func (e InvalidMessageError) Error() string {
	if e.err == nil {
		return e.msg
	}

	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e InvalidMessageError) Is(err error) bool {
	_, ok := err.(InvalidMessageError)

	return ok
}
