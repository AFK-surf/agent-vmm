//go:build darwin && !development && !e2e

package remoteconnector

import (
	"context"
	"errors"
	"fmt"

	"github.com/keybase/go-keychain"
)

type KeychainStore struct{}

func NewCredentialStore(string) CredentialStore { return KeychainStore{} }

func keychainQuery(account string) keychain.Item {
	item := keychain.NewItem()
	item.SetSecClass(keychain.SecClassGenericPassword)
	item.SetService(credentialService)
	item.SetAccount(account)
	return item
}

func (KeychainStore) Load(_ context.Context, account string) ([]byte, error) {
	query := keychainQuery(account)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnData(true)
	results, err := keychain.QueryItem(query)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCredentialUnavailable, err)
	}
	if len(results) != 1 || len(results[0].Data) == 0 {
		return nil, ErrCredentialUnavailable
	}
	return append([]byte(nil), results[0].Data...), nil
}

func (KeychainStore) Replace(_ context.Context, account string, credential []byte) error {
	if len(credential) == 0 || len(credential) > MaxCredentialBytes {
		return fmt.Errorf("credential size is invalid")
	}
	query := keychainQuery(account)
	update := keychain.NewItem()
	update.SetData(credential)
	if err := keychain.UpdateItem(query, update); err == nil {
		return nil
	} else if !errors.Is(err, keychain.ErrorItemNotFound) {
		return fmt.Errorf("keychain credential update failed: %w", err)
	}
	item := keychainQuery(account)
	item.SetData(credential)
	if err := keychain.AddItem(item); err != nil {
		return fmt.Errorf("keychain credential add failed: %w", err)
	}
	return nil
}

func (KeychainStore) Delete(_ context.Context, account string) error {
	err := keychain.DeleteItem(keychainQuery(account))
	if errors.Is(err, keychain.ErrorItemNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("keychain credential deletion failed: %w", err)
	}
	return nil
}
