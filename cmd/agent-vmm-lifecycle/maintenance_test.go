package main

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func maintenanceFixture(t *testing.T) (paths, maintenanceRequest, maintenanceActions) {
	t.Helper()
	value := testPaths(t.TempDir())
	value.app = filepath.Join(value.home, "Library", "Application Support", "Agent VMM Host", "current", "Agent VMM Host.app")
	for _, path := range []string{value.app, value.applicationData, value.logs} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	request := maintenanceRequest{Version: 1, RequestID: "maint-1", Action: "reinstall", DataPolicy: "preserve", TargetApp: value.app, SourceApp: filepath.Join(t.TempDir(), "Agent VMM Host.app"), TargetReleaseID: "release-1", ArtifactSHA256: strings.Repeat("a", 64), ArtifactSize: 100, ConfirmSharedHost: true}
	actions := maintenanceActions{
		validateSource: func(maintenanceRequest) error { return nil }, stop: func(paths) error { return nil },
		removeSoftware: func(p paths) error { return os.RemoveAll(p.app) },
		resetData: func(p paths) error {
			if err := os.RemoveAll(p.applicationData); err != nil {
				return err
			}
			return os.RemoveAll(p.logs)
		},
		replace: func(p paths, r maintenanceRequest) error { return os.MkdirAll(p.app, 0700) },
		restore: func(paths, string) error { return nil }, verify: func(paths, maintenanceRequest) error { return nil },
	}
	return value, request, actions
}

func TestMaintenanceSameVersionReinstallPreservesIdentityAndReallyReplaces(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	key := filepath.Join(p.applicationData, "device-root.key")
	if err := os.WriteFile(key, []byte("private identity"), 0600); err != nil {
		t.Fatal(err)
	}
	replaces := 0
	a.replace = func(paths, maintenanceRequest) error { replaces++; return nil }
	got, err := runMaintenance(p, r, a)
	if err != nil || got.Outcome != "succeeded" || replaces != 1 {
		t.Fatalf("%+v replacements=%d err=%v", got, replaces, err)
	}
	data, err := os.ReadFile(key)
	if err != nil || string(data) != "private identity" {
		t.Fatalf("identity changed: %q %v", data, err)
	}
	if _, err := runMaintenance(p, r, a); err != nil || replaces != 1 {
		t.Fatalf("terminal replay repeated replacement: %d %v", replaces, err)
	}
}

func TestMaintenanceResetUninstallSurvivesFailedRemovalAndNeverRestores(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	r.Action, r.DataPolicy = "uninstall", "reset"
	r.SourceApp, r.TargetReleaseID, r.ArtifactSHA256, r.ArtifactSize = "", "", "", 0
	r.ConfirmDataDeletion = true
	resets, removes := 0, 0
	reset := a.resetData
	a.resetData = func(p paths) error { resets++; return reset(p) }
	a.removeSoftware = func(p paths) error {
		removes++
		if removes == 1 {
			return errors.New("simulated I/O failure")
		}
		return os.RemoveAll(p.app)
	}
	a.restore = func(paths, string) error { t.Fatal("uninstall restored VM"); return nil }
	a.verify = func(p paths, _ maintenanceRequest) error {
		for _, path := range []string{p.app, p.applicationData, p.logs} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				return errors.New("removed path recreated")
			}
		}
		return nil
	}
	got, err := runMaintenance(p, r, a)
	if err == nil || !got.DataCleared || got.Outcome != "failed" {
		t.Fatalf("%+v %v", got, err)
	}
	status, err := maintenanceStatus(p, "")
	if err != nil || status.RequestID != r.RequestID || !status.DataCleared {
		t.Fatalf("%+v %v", status, err)
	}
	got, err = runMaintenance(p, r, a)
	if err != nil || got.Outcome != "succeeded" || resets != 1 || removes != 2 {
		t.Fatalf("%+v resets=%d removes=%d %v", got, resets, removes, err)
	}
	if _, err := maintenanceStatus(p, r.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.applicationData); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status recreated data: %v", err)
	}
}

func TestMaintenanceStopFailureDoesNotDeleteAndBlocksOtherMutations(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	r.DataPolicy, r.ConfirmDataDeletion = "reset", true
	a.stop = func(paths) error { return errors.New("process exit unknown") }
	a.resetData = func(paths) error { t.Fatal("deleted before stop confirmation"); return nil }
	got, err := runMaintenance(p, r, a)
	if err == nil || got.Stage != "stopping" || got.DataCleared {
		t.Fatalf("%+v %v", got, err)
	}
	called := false
	if _, err := runLifecycleOperation(p, "install", "other", func() error { called = true; return nil }); err == nil || called {
		t.Fatalf("other operation bypassed maintenance: called=%v err=%v", called, err)
	}
	changed := r
	changed.RequestID = "other"
	if _, err := runMaintenance(p, changed, a); err == nil {
		t.Fatal("different operation bypassed incomplete maintenance")
	}
}

func TestMaintenanceRequestCannotChangeItsDestructiveScopeOrArtifact(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	a.stop = func(paths) error { return errors.New("stop unavailable") }
	if _, err := runMaintenance(p, r, a); err == nil {
		t.Fatal("expected failure")
	}
	for _, change := range []func(*maintenanceRequest){
		func(x *maintenanceRequest) { x.DataPolicy = "reset"; x.ConfirmDataDeletion = true },
		func(x *maintenanceRequest) { x.Action = "update" },
		func(x *maintenanceRequest) { x.ArtifactSHA256 = strings.Repeat("b", 64) },
		func(x *maintenanceRequest) { x.TargetReleaseID = "different" },
	} {
		x := r
		change(&x)
		if _, err := runMaintenance(p, x, a); err == nil {
			t.Fatalf("accepted changed request %+v", x)
		}
	}
}

func TestMaintenanceUninstallSuppressesOldIntentAndExplicitReinstallAllowsNewSetup(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	oldDir := filepath.Join(p.applicationData, "install-completions")
	if err := os.MkdirAll(oldDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldDir, "old-install.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	uninstall := r
	uninstall.Action = "uninstall"
	uninstall.DataPolicy, uninstall.ConfirmDataDeletion = "reset", true
	uninstall.SourceApp = ""
	uninstall.TargetReleaseID = ""
	uninstall.ArtifactSHA256 = ""
	uninstall.ArtifactSize = 0
	if _, err := runMaintenance(p, uninstall, a); err != nil {
		t.Fatal(err)
	}
	if release, err := acquireMaintenanceMutation(p, "install", "new-setup"); err == nil {
		release()
		t.Fatal("plain install resurrected software after uninstall")
	}
	r.RequestID = "explicit-reinstall"
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	if release, err := acquireMaintenanceMutation(p, "install-managed", "old-install"); err == nil {
		release()
		t.Fatal("old managed installation replayed")
	}
	release, err := acquireMaintenanceMutation(p, "install-managed", "new-setup")
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestMaintenanceStatusDoesNotCreateItsDirectory(t *testing.T) {
	p := testPaths(t.TempDir())
	got, err := maintenanceStatus(p, "")
	if err != nil || got != nil {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := os.Stat(maintenancePaths(p)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status created journal: %v", err)
	}
}

func TestMaintenanceRefusesTargetAndResetAncestorSymlinks(t *testing.T) {
	p, r, _ := maintenanceFixture(t)
	r.TargetApp = filepath.Join(t.TempDir(), "Agent VMM Host.app")
	if err := validateMaintenanceRequest(p, r); err == nil {
		t.Fatal("accepted arbitrary deletion target")
	}
	r.TargetApp = p.app
	if err := os.RemoveAll(p.applicationData); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), p.applicationData); err != nil {
		t.Fatal(err)
	}
	if err := validateMaintenanceRequest(p, r); err == nil {
		t.Fatal("accepted symlink data target")
	}
}

func TestMaintenanceStopOldHostUsesLaunchdAndConfirmsExit(t *testing.T) {
	p, _, _ := maintenanceFixture(t)
	oldPID, oldRunning, oldDisk, oldDrain := launchctlPID, processRunning, stateDiskOpenOwners, drainVMForUpdate
	defer func() {
		launchctlPID, processRunning, stateDiskOpenOwners, drainVMForUpdate = oldPID, oldRunning, oldDisk, oldDrain
	}()
	launchctlPID = func(context.Context, string) (int, error) { return 42, nil }
	processRunning = func(int) bool { return false }
	stateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	drainVMForUpdate = func(context.Context, paths, string) error { return errors.New("old Host does not implement RPC") }
	stopped := false
	run := func(_ context.Context, args ...string) error {
		if args[0] == "print" && (stopped || strings.HasPrefix(args[1], "system/")) {
			return errLaunchctlServiceNotFound
		}
		if args[0] == "bootout" {
			stopped = true
			return nil
		}
		return nil
	}
	if err := stopForMaintenanceWith(context.Background(), p, run); err != nil || !stopped {
		t.Fatalf("stop=%v err=%v", stopped, err)
	}
}

func TestMaintenancePreserveUninstallKeepsServicePolicy(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	policy := []byte(`{"disable_pairing":true}`)
	if err := os.WriteFile(p.serviceConfigFile, policy, 0600); err != nil {
		t.Fatal(err)
	}
	r.Action = "uninstall"
	r.SourceApp = ""
	r.TargetReleaseID = ""
	r.ArtifactSHA256 = ""
	r.ArtifactSize = 0
	a.removeSoftware = defaultMaintenanceActions().removeSoftware
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p.serviceConfigFile)
	if err != nil || string(data) != string(policy) {
		t.Fatalf("preserve lost service policy: %s %v", data, err)
	}
}

func TestMaintenanceExternalLockAndReceiptSurviveReset(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	r.DataPolicy = "reset"
	r.ConfirmDataDeletion = true
	reset := a.resetData
	a.resetData = func(p paths) error {
		if err := reset(p); err != nil {
			return err
		}
		directory, err := openMaintenanceDirectory(p, false)
		if err != nil {
			return err
		}
		defer directory.Close()
		file, err := openStateFileAt(directory, "owner.lock", unix.O_RDWR, 0600, nil)
		if err != nil {
			return err
		}
		defer file.Close()
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			unix.Flock(int(file.Fd()), unix.LOCK_UN)
			t.Fatal("reset lost the operation lock")
		}
		got, err := maintenanceStatus(p, r.RequestID)
		if err != nil || got == nil || got.Stage != "resetting" {
			t.Fatalf("external receipt missing %+v %v", got, err)
		}
		return nil
	}
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceStoppedDiskStillOwnedRemainsIncomplete(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	r.DataPolicy = "reset"
	r.ConfirmDataDeletion = true
	oldDisk := stateDiskOpenOwners
	defer func() { stateDiskOpenOwners = oldDisk }()
	stateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, errors.New("disk owner unreadable") }
	a.stop = func(p paths) error {
		return stopForMaintenanceWith(context.Background(), p, func(context.Context, ...string) error { return errLaunchctlServiceNotFound })
	}
	a.resetData = func(paths) error { t.Fatal("deleted without disk release proof"); return nil }
	got, err := runMaintenance(p, r, a)
	if err == nil || got.Stage != "stopping" || got.DataCleared {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestMaintenanceCompletionCommitFailureCannotReportSuccess(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	a.commitState = func(*os.File, maintenanceState) error { return errors.New("injected state write failure") }
	got, err := runMaintenance(p, r, a)
	if err == nil || got.Outcome != "failed" || !got.EffectsVerified {
		t.Fatalf("%+v %v", got, err)
	}
	status, err := maintenanceStatus(p, r.RequestID)
	if err != nil || status.Outcome == "succeeded" {
		t.Fatalf("uncommitted success %+v %v", status, err)
	}
	a.stop = func(paths) error { t.Fatal("settlement repeated stop"); return nil }
	a.restore = func(paths, string) error { t.Fatal("settlement repeated restore"); return nil }
	a.verify = func(paths, maintenanceRequest) error { t.Fatal("settlement repeated effects"); return nil }
	a.commitState = nil
	if got, err = runMaintenance(p, r, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("%+v %v", got, err)
	}
	release, err := acquireMaintenanceMutation(p, "install", "new-install")
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestMaintenanceStatusProjectsCommittedCompletionWithoutWriting(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	got, err := runMaintenance(p, r, a)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := openMaintenanceDirectory(p, false)
	if err != nil {
		t.Fatal(err)
	}
	// Crash after the atomic owner settlement, before its receipt projection.
	got.Outcome, got.Stage = "pending", "verifying"
	if err := writeMaintenanceJSON(dir, maintenanceReceiptName(r.RequestID), got); err != nil {
		t.Fatal(err)
	}
	dir.Close()
	status, err := maintenanceStatus(p, r.RequestID)
	if err != nil || status.Outcome != "succeeded" {
		t.Fatalf("%+v %v", status, err)
	}
	dir, err = openMaintenanceDirectory(p, false)
	if err != nil {
		t.Fatal(err)
	}
	var stored maintenanceReceipt
	if err := readMaintenanceJSON(dir, maintenanceReceiptName(r.RequestID), &stored); err != nil {
		t.Fatal(err)
	}
	dir.Close()
	if stored.Outcome != "pending" {
		t.Fatal("status wrote the receipt")
	}
	// A later uninstall cannot be undone by replay of this historical request.
	next := r
	next.RequestID = "uninstall-later"
	next.Action = "uninstall"
	next.SourceApp = ""
	next.TargetReleaseID = ""
	next.ArtifactSHA256 = ""
	next.ArtifactSize = 0
	if _, err := runMaintenance(p, next, a); err != nil {
		t.Fatal(err)
	}
	a.stop = func(paths) error { t.Fatal("historical request stopped Host"); return nil }
	a.restore = func(paths, string) error { t.Fatal("historical request restored Host"); return nil }
	if got, err := runMaintenance(p, r, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("%+v %v", got, err)
	}
	if release, err := acquireMaintenanceMutation(p, "install", "new-install"); err == nil {
		release()
		t.Fatal("historical completion removed uninstall fence")
	}
}

func TestMaintenanceOldUnfinishedReceiptCannotReplaceNewIntent(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	old := r
	old.RequestID = "old-orphan"
	receipt := maintenanceReceipt{maintenanceRequest: old, ServiceMode: p.service.Mode, ServiceUser: p.service.User, Outcome: "pending", Stage: "accepted"}
	dir, err := openMaintenanceDirectory(p, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeMaintenanceJSON(dir, maintenanceReceiptName(old.RequestID), receipt); err != nil {
		t.Fatal(err)
	}
	dir.Close()
	a.stop = func(paths) error { t.Fatal("old intent repeated effects"); return nil }
	if _, err := runMaintenance(p, old, a); err == nil {
		t.Fatal("old unfinished intent replaced latest maintenance")
	}
}

func TestMaintenanceRestoreRetryDoesNotStopOrReplaceInstalledSoftware(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	stops, replaces := 0, 0
	var starts []string
	a.stop = func(paths) error { stops++; return nil }
	a.replace = func(paths, maintenanceRequest) error { replaces++; return nil }
	a.restore = func(_ paths, id string) error {
		starts = append(starts, id)
		if len(starts) == 1 {
			return errors.New("start attempt failed")
		}
		return nil
	}
	got, err := runMaintenance(p, r, a)
	if err == nil || !got.SoftwareInstalled || got.Stage != "restoring" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	if stops != 1 || replaces != 1 || len(starts) != 2 || starts[0] == starts[1] {
		t.Fatalf("stops=%d replacements=%d starts=%v", stops, replaces, starts)
	}
}

func TestMaintenanceRecoversDamagedReplacementWithoutResettingDataAgain(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	r.DataPolicy, r.ConfirmDataDeletion = "reset", true
	resets, replaces, stops, restores := 0, 0, 0, 0
	a.resetData = func(paths) error { resets++; return nil }
	a.replace = func(paths, maintenanceRequest) error { replaces++; return nil }
	a.stop = func(paths) error { stops++; return nil }
	a.restore = func(paths, string) error {
		restores++
		if restores == 1 {
			return errors.New("interrupted start")
		}
		return nil
	}
	if _, err := runMaintenance(p, r, a); err == nil {
		t.Fatal("expected interrupted restore")
	}
	a.validateInstalled = func(paths, maintenanceRequest) error { return errors.New("installed app damaged") }
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	if resets != 1 || replaces != 2 || stops != 2 {
		t.Fatalf("reset=%d replacement=%d stops=%d", resets, replaces, stops)
	}
}

func TestMaintenancePreserveUninstallAllowsRetainedBindingAfterExplicitReinstall(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	path := filepath.Join(p.applicationData, "install-completions")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "retained-install.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	uninstall := r
	uninstall.Action = "uninstall"
	uninstall.SourceApp = ""
	uninstall.TargetReleaseID = ""
	uninstall.ArtifactSHA256 = ""
	uninstall.ArtifactSize = 0
	if _, err := runMaintenance(p, uninstall, a); err != nil {
		t.Fatal(err)
	}
	if release, err := acquireMaintenanceMutation(p, "install-managed", "retained-install"); err == nil {
		release()
		t.Fatal("uninstall was undone without explicit reinstall")
	}
	r.RequestID = "explicit-preserve-reinstall"
	if _, err := runMaintenance(p, r, a); err != nil {
		t.Fatal(err)
	}
	release, err := acquireMaintenanceMutation(p, "install-managed", "retained-install")
	if err != nil {
		t.Fatal(err)
	}
	release()
}
