package keychain

import (
	"context"
	"errors"
)

const (
	Service     = "com.afk-surf.agent-vmm-host"
	AccessGroup = "D9AAN3VJK8." + Service
)

var ErrUnavailable = errors.New("SECRET_STORE_UNAVAILABLE")

type PasswordStore interface {
	Put(context.Context, string, []byte) error
	Replace(context.Context, string, []byte) error
	Get(context.Context, string) ([]byte, error)
	Delete(context.Context, string) error
	DeletePrefix(context.Context, string) error
}

type KeyStore interface {
	EnsureKey(context.Context, string, string) ([]byte, string, error)
	Sign(context.Context, string, []byte) ([]byte, error)
	DeleteKey(context.Context, string) error
}

type Backend struct{ namespace string }

func New(namespace string) *Backend                    { return &Backend{namespace: namespace} }
func (backend *Backend) account(account string) string { return backend.namespace + "/" + account }
