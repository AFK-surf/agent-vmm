//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPersistedLifecycleOperationHasNoReleaseSelector(t *testing.T) {
	value := testPaths(t.TempDir())
	operation := lifecycleOperation{
		OperationID: "operation-1", RequestID: "operation-1", Kind: "install-managed",
		Outcome: "pending", Attempt: 1, StartedAt: 1, UpdatedAt: 1,
	}
	if err := persistOperation(value, operation); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(value.applicationData, maintenanceReceiptName(operation.RequestID)))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"targetRef", "targetRevision"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("persisted operation contains retired release selector %q: %s", forbidden, data)
		}
	}
}

func TestDaemonRootLifecycleOperationPublishesServiceOwnedState(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service.Mode = serviceModeDaemon
	value.service.UID, value.service.GID = testServiceIdentity(t)
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	release, err := acquireOperationLockOwned(value, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	operation := lifecycleOperation{
		OperationID: "operation-owned", RequestID: "request-owned", Kind: "repair",
		Outcome: "pending", Attempt: 1, StartedAt: 1, UpdatedAt: 1,
	}
	if err := persistOperationOwned(value, operation, owner); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{value.applicationData, filepath.Join(value.applicationData, "owner.lock"), filepath.Join(value.applicationData, maintenanceReceiptName(operation.RequestID))} {
		assertFileOwner(t, path, value.service.UID, value.service.GID)
	}
}

func TestLifecycleOperationStateDoesNotFollowSymlinks(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	value.service.Mode = serviceModeDaemon
	value.service.UID = os.Geteuid()
	value.service.GID = os.Getegid()
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	victimDirectory := filepath.Join(root, "victim")
	if err := os.MkdirAll(victimDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(value.applicationData); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.applicationData), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victimDirectory, value.applicationData); err != nil {
		t.Fatal(err)
	}
	operation := lifecycleOperation{OperationID: "operation-symlink", RequestID: "request-symlink", Kind: "repair", Outcome: "pending"}
	if err := persistOperationOwned(value, operation, owner); err == nil {
		t.Fatal("symlinked lifecycle state directory was accepted")
	}
	if _, err := os.Stat(filepath.Join(victimDirectory, maintenanceReceiptName(operation.RequestID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target was changed: %v", err)
	}

	if err := os.Remove(value.applicationData); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	victimFile := filepath.Join(victimDirectory, "victim.json")
	if err := os.WriteFile(victimFile, []byte(`{"operation_id":"victim","request_id":"victim"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victimFile, filepath.Join(value.applicationData, "state.json")); err != nil {
		t.Fatal(err)
	}
	if observed := readLifecycleOperation(value); observed != nil {
		t.Fatalf("symlinked state was read: %+v", observed)
	}
	if err := persistOperationOwned(value, operation, owner); err == nil {
		t.Fatal("symlinked owner state was replaced")
	}

}

func TestLifecycleOperationLockRejectsFIFOWithoutBlocking(t *testing.T) {
	value := testPaths(t.TempDir())
	directory, err := openApplicationDataDirectory(value, nil)
	if err != nil {
		t.Fatal(err)
	}
	directory.Close()
	if err := unix.Mkfifo(filepath.Join(value.applicationData, "owner.lock"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		release, err := acquireOperationLock(value)
		if err == nil {
			release()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("FIFO lock result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO lock blocked lifecycle availability")
	}
}

func TestUpdateDrainRequestIDsAreUnique(t *testing.T) {
	first := newOperationID("update-drain")
	second := newOperationID("update-drain")
	if first == second {
		t.Fatalf("retry reused request id %q", first)
	}
}

func TestTerminalOperationPersistenceFailureReturnsUnknown(t *testing.T) {
	for _, actionFails := range []bool{false, true} {
		t.Run(fmt.Sprint(actionFails), func(t *testing.T) {
			value := testPaths(t.TempDir())
			operation, err := runUpdateLifecycleOperationForArtifact(value, "update-write-failed", "release-a", strings.Repeat("a", 64), 1024, func() error {
				path := filepath.Join(value.applicationData, "state.json")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if actionFails {
					return updatePreparationError{descriptor: updatePreparationFailureDescriptor{Version: 1, Stage: "download", Code: "update_download_unavailable", Message: "network unavailable", Retryable: true}}
				}
				return nil
			})
			if err == nil || operation.Outcome != "unknown" {
				t.Fatalf("operation=%+v err=%v", operation, err)
			}
			data, readErr := os.ReadFile(filepath.Join(value.applicationData, maintenanceReceiptName(operation.RequestID)))
			if readErr != nil {
				t.Fatal(readErr)
			}
			var durable lifecycleOperation
			if err := json.Unmarshal(data, &durable); err != nil || durable.Outcome != "pending" {
				t.Fatalf("durable operation=%+v err=%v", durable, err)
			}
		})
	}
}
