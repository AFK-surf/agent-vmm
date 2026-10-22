//go:build darwin || linux

package host

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

var emptyTestStore []byte

const testStoreSchemaEnv = "AGENT_VMM_TEST_STORE_SCHEMA"

func TestMain(m *testing.M) {
	path := os.Getenv(testStoreSchemaEnv)
	var directory string
	var err error
	if path == "" {
		directory, err = os.MkdirTemp("", "agent-vmm-host-test-schema-")
		if err == nil {
			path = filepath.Join(directory, "host.db")
			var database *store.Store
			database, err = store.Open(path)
			if err == nil {
				err = database.Close()
			}
			if err == nil {
				err = os.Setenv(testStoreSchemaEnv, path)
			}
		}
	}
	if err == nil {
		emptyTestStore, err = os.ReadFile(path)
	}
	if err != nil {
		if directory != "" {
			_ = os.RemoveAll(directory)
		}
		fmt.Fprintln(os.Stderr, "prepare Host test database:", err)
		os.Exit(1)
	}
	code := m.Run()
	if directory != "" {
		_ = os.RemoveAll(directory)
	}
	os.Exit(code)
}

// Each test owns a separate database. Only empty schema construction is shared;
// child test processes read the same source, and Store.Open still performs real
// startup recovery on every open and restart.
func openTestStore(path string) (*store.Store, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, err = file.Write(emptyTestStore)
		closeErr := file.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if !os.IsExist(err) {
		return nil, err
	}
	return store.Open(path)
}

func hostLockWatchdog(t *testing.T) func() {
	t.Helper()
	timer := time.AfterFunc(5*time.Second, func() {
		panic(t.Name() + ": Host lifecycle lock operation did not finish")
	})
	return func() { timer.Stop() }
}
