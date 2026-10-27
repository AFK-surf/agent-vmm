package appliancebootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
)

type SeedStore struct{ Directory string }

type durableIdentity struct {
	InstallationID string `json:"installation_id"`
	RegistrationID string `json:"registration_id"`
}

func (store SeedStore) Initialize(claim *appliancev1.ApplianceLaunchClaim) error {
	if err := os.MkdirAll(store.Directory, 0700); err != nil {
		return err
	}
	for _, base := range []string{"identity.json", "bootstrap.json"} {
		paths, err := filepath.Glob(filepath.Join(store.Directory, "."+base+".*.tmp"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	body, err := json.Marshal(durableIdentity{claim.GetInstallationId(), claim.GetRegistrationId()})
	if err != nil {
		return err
	}
	return atomicfile.PublishOnce(filepath.Join(store.Directory, "identity.json"), body, 0600)
}

type durableBootstrap struct {
	ID   string `json:"id"`
	Seed []byte `json:"seed"`
}

func (store SeedStore) Accept(_ *appliancev1.ApplianceLaunchClaim, envelope *appliancev1.OpaqueBootstrapEnvelope) error {
	path := filepath.Join(store.Directory, "bootstrap.json")
	if envelope == nil {
		_, err := os.Stat(path)
		return err
	}
	next := durableBootstrap{ID: envelope.GetBootstrapId(), Seed: envelope.GetCiphertext()}
	previous, err := os.ReadFile(path)
	if err == nil {
		var current durableBootstrap
		if err := json.Unmarshal(previous, &current); err != nil {
			return err
		}
		defer clear(current.Seed)
		if current.ID == next.ID {
			if !bytes.Equal(current.Seed, next.Seed) {
				return errors.New("bootstrap ID has a different seed")
			}
			return nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	defer clear(body)
	return atomicfile.ReplaceFile(path, body, 0600)
}

func (store SeedStore) Seed() ([]byte, error) {
	body, err := os.ReadFile(filepath.Join(store.Directory, "bootstrap.json"))
	if err != nil {
		return nil, err
	}
	defer clear(body)
	var bootstrap durableBootstrap
	if err := json.Unmarshal(body, &bootstrap); err != nil {
		return nil, err
	}
	return bootstrap.Seed, nil
}
