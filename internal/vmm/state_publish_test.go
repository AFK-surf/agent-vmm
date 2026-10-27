//go:build darwin || linux

package vmm

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStatePublicationRecoversPartialAndPreservesExistingDisk(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	free, err := freeBytes(directory)
	if err != nil {
		t.Fatal(err)
	}
	if free < uint64(hostEmergencyBytes)+(1<<20) {
		t.Skip("state publication requires 10 GiB emergency space")
	}
	partial := filepath.Join(directory, "state.raw.partial")
	if err := os.WriteFile(partial, []byte("interrupted allocation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "state.initializing"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	const size = 1 << 20
	initializing, err := prepareState(directory, size)
	if err != nil || !initializing {
		t.Fatalf("initializing=%v error=%v", initializing, err)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("partial remains: %v", err)
	}
	state := filepath.Join(directory, "state.raw")
	file, err := os.OpenFile(state, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("preserved workload")
	if _, err := file.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := finishStateInitialization(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partial, []byte("unused partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	initializing, err = prepareState(directory, size*2)
	if err != nil || initializing {
		t.Fatalf("existing initializing=%v error=%v", initializing, err)
	}
	got, err := os.ReadFile(state)
	if err != nil || len(got) != size || !bytes.Equal(got[:len(data)], data) {
		t.Fatalf("existing state changed: size=%d error=%v", len(got), err)
	}
}
