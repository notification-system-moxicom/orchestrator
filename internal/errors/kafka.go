package errors

import (
	"errors"
	"fmt"
)

type KafkaError struct {
	msg string
	err error
}

func NewKafkaError(msg string, err error) KafkaError {
	return KafkaError{msg: msg, err: err}
}

func (e KafkaError) Error() string {
	if e.err == nil {
		return e.msg
	}

	return fmt.Sprintf("%s: %s", e.msg, e.err.Error())
}

func (e KafkaError) Is(err error) bool {
	var kafkaError KafkaError

	ok := errors.As(err, &kafkaError)

	return ok
}
