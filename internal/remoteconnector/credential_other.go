//go:build !darwin && !linux

package remoteconnector

import (
	"context"
	"errors"
)

func NewCredentialStore(directory string) CredentialStore { return unavailableCredentialStore{} }

type unavailableCredentialStore struct{}

func (unavailableCredentialStore) Load(context.Context, string) ([]byte, error) {
	return nil, ErrCredentialUnavailable
}
func (unavailableCredentialStore) Replace(context.Context, string, []byte) error {
	return errors.New("controller credential storage requires a macOS or Linux host")
}
func (unavailableCredentialStore) Delete(context.Context, string) error {
	return errors.New("controller credential storage requires a macOS or Linux host")
}
