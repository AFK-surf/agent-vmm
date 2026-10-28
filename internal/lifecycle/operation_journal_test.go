//go:build darwin || linux

package lifecycle

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOwnerAdmissionPreservesUnfinishedRequestAndOldReceipts(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	pending := lifecycleOperation{OperationID: "operation-update", RequestID: "update-1", Kind: "update", TargetReleaseID: "release-2", Stage: "replace", Outcome: "pending"}
	if err := persistOperation(value, pending); err != nil {
		t.Fatal(err)
	}
	called := false
	got, err := runRepairLifecycleOperation(value, "repair-2", "", func() error { called = true; return nil })
	var busy ownerBusyError
	if !errors.As(err, &busy) || busy.requestID != pending.RequestID || got.RequestID != pending.RequestID || called {
		t.Fatalf("got=%+v err=%v called=%v", got, err, called)
	}
	pending.Outcome = "succeeded"
	if err := persistOperation(value, pending); err != nil {
		t.Fatal(err)
	}
	if _, err := runRepairLifecycleOperation(value, "repair-2", "", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	previous, err := readLifecycleOperationByID(value, pending.OperationID)
	if err != nil || previous.RequestID != pending.RequestID || previous.Outcome != "succeeded" {
		t.Fatalf("previous=%+v err=%v", previous, err)
	}
}

func TestUnreadableCurrentReceiptStopsAdmission(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	operation := lifecycleOperation{OperationID: "operation-broken", RequestID: "broken", Kind: "repair", Outcome: "pending"}
	if err := persistOperation(value, operation); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(value.applicationData, maintenanceReceiptName(operation.RequestID))
	if err := os.WriteFile(path, []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	if _, err := runRepairLifecycleOperation(value, "broken", "", func() error { called = true; return nil }); err == nil || called {
		t.Fatalf("accepted unreadable receipt: called=%t err=%v", called, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "{broken" {
		t.Fatalf("receipt changed: %q %v", data, err)
	}
}

func TestHostResetPreservesJournalAndDoesNotFollowDataSymlinks(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(value.applicationData, 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	retained := filepath.Join(outside, "retained")
	if err := os.WriteFile(retained, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(value.applicationData, "linked")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"owner.lock", "state.json", maintenanceReceiptName("request-1"), "host.sqlite"} {
		if err := os.WriteFile(filepath.Join(value.applicationData, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeHostData(value); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"owner.lock", "state.json", maintenanceReceiptName("request-1")} {
		if _, err := os.Stat(filepath.Join(value.applicationData, name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Lstat(filepath.Join(value.applicationData, "host.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Host database remains: %v", err)
	}
	if data, err := os.ReadFile(retained); err != nil || string(data) != "outside" {
		t.Fatalf("unrelated data changed: %s %v", data, err)
	}
}

func TestTerminalManagedInstallReceiptSurvivesLaterUpdate(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	operationID := "operation-terminal-install"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0700); err != nil {
		t.Fatal(err)
	}
	plan := []byte(`{"material":"secret-token"}`)
	if err := os.WriteFile(planPath, plan, 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	first, err := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		calls++
		return terminalManagedInstallError(os.ErrPermission)
	})
	if err == nil || first.Retryable || first.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if _, err := runUpdateLifecycleOperation(value, "update-after-install", "release-2", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	latest := readLifecycleOperation(value)
	if latest == nil || latest.Kind != "update" || latest.Outcome != "succeeded" {
		t.Fatalf("latest=%+v", latest)
	}
	resumed, resumeErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		calls++
		return nil
	})
	if resumeErr == nil || calls != 1 || resumed.Attempt != first.Attempt {
		t.Fatalf("terminal action replayed: resumed=%+v calls=%d err=%v", resumed, calls, resumeErr)
	}
	finalized, finalErr := finalizeManagedInstallOperation(value, operationID, resumed, resumeErr)
	if finalErr != resumeErr || finalized.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("terminal receipt lost: finalized=%+v err=%v", finalized, finalErr)
	}
	if retained, err := os.ReadFile(planPath); err != nil || string(retained) != string(plan) {
		t.Fatalf("retained plan=%q err=%v", retained, err)
	}
	recorded, err := readLifecycleOperationByID(value, operationID)
	if err != nil || recorded.Outcome != "failed" || recorded.Attempt != 1 || recorded.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("recorded=%+v err=%v", recorded, err)
	}
}
