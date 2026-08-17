package mcpappliance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunPublishesOnlyPrivateContractSockets(t *testing.T) {
	t.Parallel()
	shortDirectory, err := os.MkdirTemp("/tmp", "mcpapp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(shortDirectory) })
	runtimeDirectory := filepath.Join(shortDirectory, "run")
	stateDirectory := filepath.Join(t.TempDir(), "state")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Config{RuntimeDirectory: runtimeDirectory, StateDirectory: stateDirectory}) }()
	for _, name := range []string{BootstrapSocket, ControllerSocket, FrontendHTTPSocket} {
		path := filepath.Join(runtimeDirectory, name)
		deadline := time.Now().Add(2 * time.Second)
		for {
			info, err := os.Stat(path)
			if err == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm() == 0o600 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s was not published privately: %v", name, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := os.Stat(filepath.Join(runtimeDirectory, "lifecycle.sock")); !os.IsNotExist(err) {
		t.Fatalf("obsolete lifecycle socket exists: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("appliance did not stop")
	}
}
