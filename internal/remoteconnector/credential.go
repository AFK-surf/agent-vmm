package remoteconnector

import (
	"context"
	"errors"
)

var ErrCredentialUnavailable = errors.New("controller credential is unavailable")

type CredentialStore interface {
	Load(context.Context, string) ([]byte, error)
	Replace(context.Context, string, []byte) error
	Delete(context.Context, string) error
}

func wipe(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
