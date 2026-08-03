package control

import (
	"errors"
	"fmt"
)

var (
	errResourceExhausted = errors.New("resource exhausted")
	errPrecondition      = errors.New("failed precondition")
)

func resourceExhausted(message string) error {
	return fmt.Errorf("%w: %s", errResourceExhausted, message)
}

func failedPrecondition(message string) error {
	return fmt.Errorf("%w: %s", errPrecondition, message)
}
