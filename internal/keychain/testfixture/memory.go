package testfixture

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
)

type Passwords struct {
	mu     sync.Mutex
	values map[string][]byte
}

func NewPasswords() *Passwords { return &Passwords{values: make(map[string][]byte)} }
func (store *Passwords) Put(_ context.Context, account string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if current, ok := store.values[account]; ok && !bytes.Equal(current, value) {
		return os.ErrExist
	}
	store.values[account] = bytes.Clone(value)
	return nil
}
func (store *Passwords) Replace(_ context.Context, account string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.values[account] = bytes.Clone(value)
	return nil
}
func (store *Passwords) Get(_ context.Context, account string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.values[account]
	if !ok {
		return nil, os.ErrNotExist
	}
	return bytes.Clone(value), nil
}
func (store *Passwords) Delete(_ context.Context, account string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.values, account)
	return nil
}
func (store *Passwords) DeletePrefix(_ context.Context, prefix string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	for account := range store.values {
		if strings.HasPrefix(account, prefix) {
			delete(store.values, account)
		}
	}
	return nil
}
