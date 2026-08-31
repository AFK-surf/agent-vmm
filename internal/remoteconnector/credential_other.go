//go:build !darwin

package remoteconnector

import (
	"context"
	"errors"
)

func NewCredentialStore() CredentialStore { return unavailableCredentialStore{} }

type unavailableCredentialStore struct{}

func (unavailableCredentialStore) Load(context.Context, string) ([]byte, error) {
	return nil, ErrCredentialUnavailable
}
func (unavailableCredentialStore) Replace(context.Context, string, []byte) error {
	return errors.New("macOS controller credential storage is required")
}
func (unavailableCredentialStore) Delete(context.Context, string) error {
	return errors.New("macOS controller credential storage is required")
}
