//go:build darwin || linux

package lifecycle

import (
	"errors"
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
	data, err := os.ReadFile(value.operationFile)
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
	if err := os.WriteFile(value.operationFile, []byte(`{"operation_id":"operation-old","request_id":"request-old","kind":"repair","outcome":"succeeded"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	release, err := acquireOperationLockOwned(value, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	assertFileOwner(t, value.operationFile, value.service.UID, value.service.GID)
	operation := lifecycleOperation{
		OperationID: "operation-owned", RequestID: "request-owned", Kind: "repair",
		Outcome: "pending", Attempt: 1, StartedAt: 1, UpdatedAt: 1,
	}
	if err := persistOperationOwned(value, operation, owner); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{value.applicationData, value.operationFile + ".lock", value.operationFile} {
		assertFileOwner(t, path, value.service.UID, value.service.GID)
	}
}

func TestDaemonRootLifecycleOperationDoesNotPublishBeforeOwnership(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service.Mode = serviceModeDaemon
	owner := lifecycleOperationOwnerFor(value, 0, func(fd, _, _ int) error {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			return nil
		}
		return os.ErrPermission
	})
	operation := lifecycleOperation{
		OperationID: "operation-unowned", RequestID: "request-unowned", Kind: "repair",
		Outcome: "pending", Attempt: 1, StartedAt: 1, UpdatedAt: 1,
	}
	if err := persistOperationOwned(value, operation, owner); err == nil {
		t.Fatal("ownership failure published lifecycle state")
	}
	if _, err := os.Stat(value.operationFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned lifecycle state became visible: %v", err)
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
	if _, err := os.Stat(filepath.Join(victimDirectory, filepath.Base(value.operationFile))); !errors.Is(err, os.ErrNotExist) {
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
	if err := os.Symlink(victimFile, value.operationFile); err != nil {
		t.Fatal(err)
	}
	if observed := readOperation(value.operationFile); observed != nil {
		t.Fatalf("symlinked operation was read: %+v", observed)
	}
	if release, err := acquireOperationLockOwned(value, owner); err == nil {
		release()
		t.Fatal("symlinked operation was handed to the service user")
	}
}

func TestLifecycleOperationLockRejectsFIFOWithoutBlocking(t *testing.T) {
	value := testPaths(t.TempDir())
	directory, err := openApplicationDataDirectory(value, nil)
	if err != nil {
		t.Fatal(err)
	}
	directory.Close()
	if err := unix.Mkfifo(value.operationFile+".lock", 0o600); err != nil {
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
