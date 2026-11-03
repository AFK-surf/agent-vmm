//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestExactDrainFencesOldClaimButAllowsExplicitEnable(t *testing.T) {
	value := testPaths(t.TempDir())
	run := func(kind, request, target string, action func() error) (lifecycleOperation, error) {
		return runLifecycleOperationWithFailedRetry(value, kind, request, target, "", "", 0, true, action)
	}
	if _, err := run("registration-claim", "claim", "target", func() error { return context.DeadlineExceeded }); err == nil {
		t.Fatal("claim failure missing")
	}
	if _, err := run("registration-claim", "neighbor-claim", "neighbor", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := run("registration-drain", "drain", "target", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	called := false
	if receipt, err := run("registration-claim", "claim", "target", func() error { called = true; return nil }); err == nil || called || receipt.Outcome != "superseded" {
		t.Fatalf("old claim replay: %+v %v called=%v", receipt, err, called)
	}
	if _, err := run("registration-enable", "enable", "target", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := run("registration-remove", "remove", "target", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := run("registration-enable", "later-enable", "target", func() error { t.Fatal("removed registration revived"); return nil }); err == nil {
		t.Fatal("remove tombstone was lost")
	}
	if prior, err := readLifecycleOperationByID(value, "neighbor-claim"); err != nil || prior.Outcome != "succeeded" {
		t.Fatalf("neighbor changed: %+v %v", prior, err)
	}
}
func TestSupersessionWaitsForOSOwnerBeforeWritingFence(t *testing.T) {
	value := testPaths(t.TempDir())
	if _, err := runLifecycleOperationWithFailedRetry(value, "registration-claim", "claim", "target", "", "", 0, true, func() error { return context.DeadlineExceeded }); err == nil {
		t.Fatal("claim failure missing")
	}
	release, err := acquireOperationLock(value)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := runLifecycleOperationWithFailedRetry(value, "registration-remove", "remove", "target", "", "", 0, true, func() error { return nil })
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("lock acquisition returned before release: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	prior, err := readLifecycleOperationByID(value, "claim")
	if err != nil || prior.Outcome != "failed" {
		t.Fatalf("fence preceded OS ownership: %+v %v", prior, err)
	}
	release()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	prior, err = readLifecycleOperationByID(value, "claim")
	if err != nil || prior.Outcome != "superseded" {
		t.Fatalf("old request unfenced: %+v %v", prior, err)
	}
}

func TestLegacyRegistrationReadRetainsOriginalRequestWithoutAlias(t *testing.T) {
	value := testPaths(t.TempDir())
	original := lifecycleOperation{OperationID: "old-operation", RequestID: "old-native-request", Kind: "install-managed", Outcome: "failed", Retryable: true}
	if err := persistOperation(value, original); err != nil {
		t.Fatal(err)
	}
	planDir, err := openInstallOperationDirectory(value, original.RequestID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = atomicWriteStateFileAt(planDir, "plan.json", []byte(`{"version":1,"material":{"registration_id":"registration"}}`), 0600, nil)
	planDir.Close()
	if err != nil {
		t.Fatal(err)
	}
	if report, err := readCorrelatedOperation(value, "new-request", "registration"); err != nil || report != nil {
		t.Fatalf("unaccepted request guessed from target: %v %v", report, err)
	}
	value.correlationRequestID = "new-request"
	operation, err := runLifecycleOperationWithFailedRetry(value, "registration-recover", original.RequestID, "registration", "", "", 0, true, func() error { return nil })
	if err != nil || operation.OperationID != original.OperationID || operation.RequestID != original.RequestID || operation.CorrelationRequestID != "new-request" || operation.Outcome != "succeeded" {
		t.Fatalf("operation=%+v err=%v", operation, err)
	}
	report, err := readCorrelatedOperation(value, "new-request", "registration")
	if err != nil || report["requestId"] != original.RequestID || report["correlationRequestId"] != "new-request" || report["targetRegistrationId"] != "registration" {
		t.Fatalf("report=%v err=%v", report, err)
	}
	if other, err := readCorrelatedOperation(value, "other-request", "registration"); err != nil || other != nil {
		t.Fatalf("old success satisfied another request: %v %v", other, err)
	}
	canonical, err := originalRegistrationRequestID(value, "registration", "next-explicit-request", "registration-recover")
	if err != nil || canonical != "next-explicit-request" {
		t.Fatalf("next request redirected to legacy owner: %s %v", canonical, err)
	}
}

func TestUnifiedReceiptReadSeparatesMissingFromUnreadableAccepted(t *testing.T) {
	value := testPaths(t.TempDir())
	if receipt, err := readOperationReportByID(value, "missing"); err != nil || receipt != nil {
		t.Fatalf("missing=%v err=%v", receipt, err)
	}
	if err := persistOperation(value, lifecycleOperation{OperationID: "operation", RequestID: "request", Kind: "registration-claim", TargetRegistrationID: "registration", Outcome: "failed"}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(value.applicationData, maintenanceReceiptName("request"))); err != nil {
		t.Fatal(err)
	}
	if receipt, err := readOperationReportByID(value, "request"); err == nil || receipt != nil {
		t.Fatalf("missing accepted=%v err=%v", receipt, err)
	}
}

func TestLegacyCorrelationRequiresAdmissionCommitAndCannotBeReassigned(t *testing.T) {
	value := testPaths(t.TempDir())
	original := lifecycleOperation{OperationID: "operation", RequestID: "original", Kind: "install-managed", Outcome: "failed", Retryable: true}
	if err := persistOperation(value, original); err != nil {
		t.Fatal(err)
	}
	directory, err := openExistingApplicationDataDirectory(value)
	if err != nil {
		t.Fatal(err)
	}
	pending := original
	pending.Kind = "registration-recover"
	pending.TargetRegistrationID = "registration"
	pending.CorrelationRequestID = "replacement"
	pending.Outcome = "pending"
	if err := writeMaintenanceJSON(directory, maintenanceReceiptName(original.RequestID), pending); err != nil {
		t.Fatal(err)
	}
	if report, err := readCorrelatedOperation(value, "replacement", "registration"); err != nil || report != nil {
		t.Fatalf("uncommitted correlation reported accepted: %v %v", report, err)
	}
	state, err := loadMaintenanceState(directory)
	if err != nil {
		t.Fatal(err)
	}
	state.RegistrationRequests["registration"] = original.RequestID
	if err := writeOwnerState(directory, state); err != nil {
		t.Fatal(err)
	}
	directory.Close()
	if report, err := readCorrelatedOperation(value, "replacement", "registration"); err != nil || report["requestId"] != original.RequestID {
		t.Fatalf("committed correlation missing: %v %v", report, err)
	}
	value.correlationRequestID = "another-replacement"
	called := false
	if _, err := runLifecycleOperationWithFailedRetry(value, "registration-recover", original.RequestID, "registration", "", "", 0, true, func() error { called = true; return nil }); err == nil || called {
		t.Fatalf("correlation reassigned: called=%v err=%v", called, err)
	}
}

func TestNewRecoveryFencesLateFailedCredentialReplacement(t *testing.T) {
	value := testPaths(t.TempDir())
	credentials := remoteconnector.NewCredentialStore(filepath.Join(t.TempDir(), "credentials"))
	replace := func(request, credential string, fail bool) (lifecycleOperation, error) {
		return runLifecycleOperationWithFailedRetry(value, "registration-recover", request, "registration", "", "", 0, true, func() error {
			if err := credentials.Replace(context.Background(), "registration", []byte(credential)); err != nil {
				return err
			}
			if fail {
				return context.DeadlineExceeded
			}
			return nil
		})
	}
	if _, err := replace("old-recovery", "old-candidate", true); err == nil {
		t.Fatal("missing interrupted Apply result")
	}
	if _, err := replace("new-recovery", "new-candidate", false); err != nil {
		t.Fatal(err)
	}
	if _, err := replace("old-recovery", "old-candidate", false); err == nil {
		t.Fatal("old recovery replayed after another request took ownership")
	}
	current, err := credentials.Load(context.Background(), "registration")
	if err != nil || string(current) != "new-candidate" {
		t.Fatalf("late recovery replaced the current credential: %q %v", current, err)
	}
	previous, err := readLifecycleOperationByID(value, "old-recovery")
	if err != nil || previous.Outcome != "failed" || previous.SupersededByRequestID != "new-recovery" || previous.FailureCode != "transient_timeout" {
		t.Fatalf("old unknown/failure evidence changed: %+v %v", previous, err)
	}
}
