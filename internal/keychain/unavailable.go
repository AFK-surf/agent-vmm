//go:build !darwin || !keychain || !cgo

package keychain

import "context"

func Available() bool                                          { return false }
func (*Backend) Probe(context.Context) error                   { return ErrUnavailable }
func (*Backend) Put(context.Context, string, []byte) error     { return ErrUnavailable }
func (*Backend) Replace(context.Context, string, []byte) error { return ErrUnavailable }
func (*Backend) Get(context.Context, string) ([]byte, error)   { return nil, ErrUnavailable }
func (*Backend) Delete(context.Context, string) error          { return ErrUnavailable }
func (*Backend) DeletePrefix(context.Context, string) error    { return ErrUnavailable }
func (*Backend) EnsureKey(context.Context, string, string) ([]byte, string, error) {
	return nil, "", ErrUnavailable
}
func (*Backend) Sign(context.Context, string, []byte) ([]byte, error) { return nil, ErrUnavailable }
func (*Backend) DeleteKey(context.Context, string) error              { return ErrUnavailable }
