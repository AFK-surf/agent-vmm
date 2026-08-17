package mcpappliance

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type durableIdentity struct {
	InstallationID string `json:"installation_id"`
	RegistrationID string `json:"registration_id"`
}

type stateStore struct{ directory string }

func (store stateStore) loadIdentity() (durableIdentity, error) {
	value, err := os.ReadFile(filepath.Join(store.directory, "identity.json"))
	if err != nil {
		return durableIdentity{}, err
	}
	var identity durableIdentity
	if err := json.Unmarshal(value, &identity); err != nil || identity.InstallationID == "" || identity.RegistrationID == "" {
		return durableIdentity{}, errors.Join(errors.New("durable appliance identity is invalid"), err)
	}
	return identity, nil
}

func (store stateStore) initializeIdentity(identity durableIdentity) error {
	if err := os.MkdirAll(store.directory, 0o700); err != nil {
		return err
	}
	path := filepath.Join(store.directory, "identity.json")
	value, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		if _, err = file.Write(value); err == nil {
			err = file.Sync()
		}
		return errors.Join(err, file.Close())
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var persisted durableIdentity
	if json.Unmarshal(existing, &persisted) != nil || persisted != identity {
		return errors.New("launch identity does not match durable appliance identity")
	}
	return nil
}

func (store stateStore) consumeBootstrap(bootstrapID string) error {
	if bootstrapID == "" {
		return errors.New("bootstrap identity is empty")
	}
	path := filepath.Join(store.directory, "bootstrap.id")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		value, readErr := os.ReadFile(path)
		if readErr == nil && string(value) == bootstrapID {
			return nil
		}
		return errors.New("bootstrap seed was already consumed")
	}
	if err != nil {
		return err
	}
	if _, err = file.WriteString(bootstrapID); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func (store stateStore) hasSeed() bool {
	info, err := os.Stat(filepath.Join(store.directory, "bootstrap.id"))
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm() == 0o600
}

func (store stateStore) saveInventory(hello *remotev1.HostHello) error {
	value, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(hello)
	if err != nil {
		return err
	}
	if len(value) > 1<<20 {
		return errors.New("authoritative inventory exceeds durable bound")
	}
	path := filepath.Join(store.directory, "inventory.json")
	temporary, err := os.CreateTemp(store.directory, ".inventory-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err = temporary.Write(value); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
