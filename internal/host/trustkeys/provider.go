package trustkeys

import (
	"context"
	"errors"
)

var ErrKeyUnavailable = errors.New("device root key is unavailable")

type Provider interface {
	Ensure(context.Context, uint64) (publicKey []byte, custody string, err error)
	Sign(context.Context, uint64, []byte) ([]byte, error)
	Delete(context.Context, uint64) error
}
