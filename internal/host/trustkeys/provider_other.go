//go:build !darwin

package trustkeys

import "context"

type unavailableProvider struct{}

func NewPlatformProvider() Provider { return unavailableProvider{} }
func (unavailableProvider) Ensure(context.Context, uint64) ([]byte, string, error) {
	return nil, "", ErrKeyUnavailable
}
func (unavailableProvider) Sign(context.Context, uint64, []byte) ([]byte, error) {
	return nil, ErrKeyUnavailable
}
func (unavailableProvider) Delete(context.Context, uint64) error { return ErrKeyUnavailable }
