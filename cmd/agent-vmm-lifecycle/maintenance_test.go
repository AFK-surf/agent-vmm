package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"golang.org/x/sys/unix"
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
		targetSchema:   func(maintenanceRequest) (store.SchemaRange, error) { return store.SupportedSchema(), nil },
		dataSchema:     func(paths) (int, error) { return 0, nil },
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

func TestMaintenanceStopCrashLoopingHostBootsOutWithoutProcessWait(t *testing.T) {
	p, _, _ := maintenanceFixture(t)
	oldObserver, oldDisk, oldDrain := launchctlServiceObserver, stateDiskOpenOwners, drainVMForUpdate
	defer func() {
		launchctlServiceObserver, stateDiskOpenOwners, drainVMForUpdate = oldObserver, oldDisk, oldDrain
	}()
	launchctlServiceObserver = func(context.Context, string) (launchctlServiceFacts, error) {
		return parseLaunchctlServiceObservation([]byte(crashLoopLaunchctlPrint))
	}
	diskChecked := false
	stateDiskOpenOwners = func(context.Context, string) ([]int, error) { diskChecked = true; return nil, nil }
	drainVMForUpdate = func(context.Context, paths, string) error {
		t.Fatal("drained the VM of a Host that has no process")
		return nil
	}
	var bootouts []string
	run := func(_ context.Context, args ...string) error {
		booted := false
		for _, target := range bootouts {
			booted = booted || target == args[1]
		}
		switch {
		case args[0] == "print" && (booted || strings.HasPrefix(args[1], "system/")):
			return errLaunchctlServiceNotFound
		case args[0] == "bootout":
			bootouts = append(bootouts, args[1])
		}
		return nil
	}
	if err := stopForMaintenanceWith(context.Background(), p, run); err != nil || len(bootouts) == 0 || bootouts[0] != p.serviceDomain()+"/"+hostLabel || !diskChecked {
		t.Fatalf("bootouts=%v diskChecked=%v err=%v", bootouts, diskChecked, err)
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

func TestMaintenancePreflightRefusesTargetOlderThanDataWithoutEffects(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	a.dataSchema = func(paths) (int, error) { return 35, nil }
	a.targetSchema = func(maintenanceRequest) (store.SchemaRange, error) { return store.SchemaRange{Min: 18, Max: 25}, nil }
	noEffect := func(paths) error { t.Fatal("refused maintenance made an effect"); return nil }
	a.stop, a.resetData, a.removeSoftware = noEffect, noEffect, noEffect
	a.replace = func(paths, maintenanceRequest) error { return noEffect(p) }
	for range 2 {
		got, err := runMaintenance(p, r, a)
		if err == nil || got.Outcome != "failed" || got.Retryable || got.FailureCode != "target_older_than_data" || got.Stage != "accepted" || got.SoftwareInstalled {
			t.Fatalf("%+v %v", got, err)
		}
	}
	status, err := maintenanceStatus(p, r.RequestID)
	if err != nil || status == nil || status.FailureCode != "target_older_than_data" {
		t.Fatalf("status = %+v %v", status, err)
	}
	if latest, err := maintenanceStatus(p, ""); err != nil || latest != nil {
		t.Fatalf("refusal changed the owner state: %+v %v", latest, err)
	}
	release, err := acquireMaintenanceMutation(p, "repair", "repair-current")
	if err != nil {
		t.Fatalf("refusal fenced the current Host: %v", err)
	}
	release()

	reset := r
	reset.RequestID = "maint-reset"
	reset.DataPolicy, reset.ConfirmDataDeletion = "reset", true
	a.dataSchema = func(paths) (int, error) { t.Fatal("reset read the data schema"); return 0, nil }
	a.stop, a.resetData, a.removeSoftware = func(paths) error { return nil }, func(paths) error { return nil }, func(paths) error { return nil }
	a.replace = func(paths, maintenanceRequest) error { return nil }
	if got, err := runMaintenance(p, reset, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("reset after refusal = %+v %v", got, err)
	}
}

func TestMaintenanceRestoreIsBoundedAndThenReplaceable(t *testing.T) {
	p, r, a := maintenanceFixture(t)
	restores := 0
	a.restore = func(paths, string) error { restores++; return errors.New("host exited") }
	for attempt := 1; attempt <= maintenanceRestoreAttempts+1; attempt++ {
		got, err := runMaintenance(p, r, a)
		final := attempt >= maintenanceRestoreAttempts
		if err == nil || got.Outcome != "failed" || got.Retryable == final || (got.FailureCode == "host_start_failed") != final || got.Stage != "restoring" {
			t.Fatalf("attempt %d = %+v %v", attempt, got, err)
		}
	}
	if restores != maintenanceRestoreAttempts {
		t.Fatalf("restores = %d", restores)
	}
	reset := r
	reset.RequestID, reset.SupersedesRequestID = "maint-reset", r.RequestID
	reset.DataPolicy, reset.ConfirmDataDeletion = "reset", true
	a.restore = func(paths, string) error { return nil }
	if got, err := runMaintenance(p, reset, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("replacement after host_start_failed = %+v %v", got, err)
	}
}

func TestRestoreHostEndsAtHostStartFailureRecord(t *testing.T) {
	value := testPaths(t.TempDir())
	record := filepath.Join(value.applicationData, store.StartFailureFile)
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, []byte(`{"code":"data_requires_conversion","dataSchema":16,"supportedSchema":{"min":18,"max":35}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	repair := func(paths) error {
		if _, err := os.Stat(record); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("stale start failure record survived the restart")
		}
		return os.WriteFile(record, []byte(`{"code":"data_newer_than_host","dataSchema":36,"supportedSchema":{"min":18,"max":35}}`), 0o600)
	}
	started := time.Now()
	err := restoreHost(value, "restore", repair, func(ctx context.Context, _ paths, _ string) error {
		<-ctx.Done()
		return errors.New("Agent VMM operator is not readable")
	})
	var failure maintenanceFailure
	if !errors.As(err, &failure) || failure.code != "host_start_failed" || !strings.Contains(err.Error(), "data_newer_than_host") || time.Since(started) > 5*time.Second {
		t.Fatalf("restore = %v after %s", err, time.Since(started))
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

// stickMaintenance leaves old as the active request in the receipt format of
// release c170350: pending at restoring, with no failure fields, and no
// executor that holds owner.lock.
func stickMaintenance(t *testing.T, p paths, old maintenanceRequest, a maintenanceActions) {
	t.Helper()
	a.restore = func(paths, string) error { return errors.New("existing data cannot start") }
	if failed, err := runMaintenance(p, old, a); err == nil || !failed.SoftwareInstalled {
		t.Fatalf("%+v %v", failed, err)
	}
	directory, unlock, err := lockMaintenance(p)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	receipt := map[string]any{"version": 1, "requestId": old.RequestID, "action": old.Action, "dataPolicy": old.DataPolicy, "targetApp": old.TargetApp,
		"sourceApp": old.SourceApp, "targetReleaseId": old.TargetReleaseID, "artifactSha256": old.ArtifactSHA256, "artifactSize": old.ArtifactSize,
		"confirmSharedHost": true, "serviceMode": p.service.Mode, "serviceUser": p.service.User, "disablePairing": false,
		"stage": "restoring", "outcome": "pending", "retryable": true, "softwareInstalled": true, "restoreAttempts": 7, "startedAt": 1, "updatedAt": 2}
	if err := writeMaintenanceJSON(directory, maintenanceReceiptName(old.RequestID), receipt); err != nil {
		t.Fatal(err)
	}
}

func TestActiveMaintenanceCanBeReplacedWithoutReplayingOldWork(t *testing.T) {
	for _, mode := range []struct {
		action, policy string
		stuck          bool
	}{{"update", "preserve", true}, {"reinstall", "preserve", false}, {"reinstall", "reset", false}, {"uninstall", "preserve", true}, {"uninstall", "reset", false}} {
		t.Run(fmt.Sprintf("%s-%s-stuck=%t", mode.action, mode.policy, mode.stuck), func(t *testing.T) {
			p, old, a := maintenanceFixture(t)
			if mode.stuck {
				stickMaintenance(t, p, old, a)
			} else {
				a.restore = func(paths, string) error { return errors.New("existing data cannot start") }
				if failed, err := runMaintenance(p, old, a); err == nil || !failed.SoftwareInstalled || failed.Outcome != "failed" {
					t.Fatalf("%+v %v", failed, err)
				}
			}
			next := old
			next.RequestID = "maint-2"
			next.SupersedesRequestID = old.RequestID
			next.Action = mode.action
			next.DataPolicy = mode.policy
			next.ConfirmDataDeletion = mode.policy == "reset"
			if next.Action == "uninstall" {
				next.SourceApp = ""
				next.TargetReleaseID = ""
				next.ArtifactSHA256 = ""
				next.ArtifactSize = 0
			}
			stops, resets := 0, 0
			a.stop = func(paths) error { stops++; return nil }
			a.resetData = func(paths) error {
				if stops == 0 {
					t.Fatal("reset before stop")
				}
				resets++
				return nil
			}
			a.restore = func(paths, string) error { return nil }
			done, err := runMaintenance(p, next, a)
			if err != nil || done.Outcome != "succeeded" || stops != 1 {
				t.Fatalf("%+v stops=%d %v", done, stops, err)
			}
			if (resets == 1) != (mode.policy == "reset") {
				t.Fatalf("resets=%d", resets)
			}
			a.stop = func(paths) error { t.Fatal("old request performed effects"); return nil }
			replay, err := runMaintenance(p, old, a)
			if err != nil || replay.Outcome != "superseded" || replay.SupersededByRequestID != next.RequestID || replay.Retryable {
				t.Fatalf("%+v %v", replay, err)
			}
			observed, err := maintenanceStatus(p, old.RequestID)
			if err != nil || observed.Outcome != "superseded" {
				t.Fatalf("%+v %v", observed, err)
			}
		})
	}
}

func TestSupersessionAdmissionWriteFailureDoesNotAcceptOrLoseEitherScope(t *testing.T) {
	p, old, a := maintenanceFixture(t)
	a.restore = func(paths, string) error { return errors.New("cannot restore data") }
	if _, err := runMaintenance(p, old, a); err == nil {
		t.Fatal("expected failure")
	}
	next := old
	next.RequestID = "next"
	next.SupersedesRequestID = old.RequestID
	next.Action = "reinstall"
	next.DataPolicy = "reset"
	next.ConfirmDataDeletion = true
	a.commitAdmission = func(*os.File, maintenanceState) error { return errors.New("state commit failed") }
	a.stop = func(paths) error { t.Fatal("effects before acceptance"); return nil }
	if _, err := runMaintenance(p, next, a); err == nil {
		t.Fatal("ignored commit failure")
	}
	observed, err := maintenanceStatus(p, next.RequestID)
	if err != nil || observed != nil {
		t.Fatalf("unaccepted receipt exposed: %+v %v", observed, err)
	}
	previous, err := maintenanceStatus(p, old.RequestID)
	if err != nil || previous.Outcome != "failed" {
		t.Fatalf("prior receipt lost: %+v %v", previous, err)
	}
	// The exact same request recovers the staged receipt, then stops afresh.
	a.commitAdmission = nil
	stops := 0
	a.stop = func(paths) error { stops++; return nil }
	a.restore = func(paths, string) error { return nil }
	done, err := runMaintenance(p, next, a)
	if err != nil || done.Outcome != "succeeded" || stops != 1 {
		t.Fatalf("%+v stops=%d %v", done, stops, err)
	}
}

func TestSupersedingStopFailureRetainsNewRequestAndNeverClearsData(t *testing.T) {
	p, old, a := maintenanceFixture(t)
	a.restore = func(paths, string) error { return errors.New("cannot restore") }
	_, _ = runMaintenance(p, old, a)
	next := old
	next.RequestID = "next"
	next.SupersedesRequestID = old.RequestID
	next.DataPolicy = "reset"
	next.ConfirmDataDeletion = true
	a.stop = func(paths) error { return errors.New("disk owner unknown") }
	a.resetData = func(paths) error { t.Fatal("reset despite failed stop"); return nil }
	got, err := runMaintenance(p, next, a)
	if err == nil || got.Outcome != "failed" || got.DataCleared {
		t.Fatalf("%+v %v", got, err)
	}
	previous, _ := maintenanceStatus(p, old.RequestID)
	latest, _ := maintenanceStatus(p, "")
	if previous.Outcome != "superseded" || latest.RequestID != next.RequestID || latest.Outcome != "failed" {
		t.Fatalf("old=%+v latest=%+v", previous, latest)
	}
}

func TestSupersessionRejectsUnconfirmedOrDifferentPredecessor(t *testing.T) {
	for _, kind := range []string{"completed", "different-service", "different-target", "stale-id"} {
		t.Run(kind, func(t *testing.T) {
			p, old, a := maintenanceFixture(t)
			a.restore = func(paths, string) error { return errors.New("cannot restore") }
			_, _ = runMaintenance(p, old, a)
			next := old
			next.RequestID = "next"
			next.SupersedesRequestID = old.RequestID
			next.DataPolicy = "reset"
			next.ConfirmDataDeletion = true
			directory, unlock, err := lockMaintenance(p)
			if err != nil {
				t.Fatal(err)
			}
			var prior maintenanceReceipt
			if err = readMaintenanceJSON(directory, maintenanceReceiptName(old.RequestID), &prior); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "completed":
				state, _ := loadMaintenanceState(directory)
				state.CompletedRequests[old.RequestID] = true
				err = writeMaintenanceJSON(directory, "state.json", state)
			case "different-service":
				prior.ServiceUser = "different-user"
			case "different-target":
				prior.TargetApp = "/other/Agent VMM Host.app"
			case "stale-id":
				next.SupersedesRequestID = "another-request"
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = writeMaintenanceJSON(directory, maintenanceReceiptName(old.RequestID), prior); err != nil {
				t.Fatal(err)
			}
			unlock()
			a.stop = func(paths) error { t.Fatal("rejected supersession performed effects"); return nil }
			if _, err = runMaintenance(p, next, a); err == nil {
				t.Fatal("accepted invalid supersession")
			}
		})
	}
}

func TestStatusDoesNotReportAnAcceptedMissingReceiptAsUnaccepted(t *testing.T) {
	p, _, _ := maintenanceFixture(t)
	directory, unlock, err := lockMaintenance(p)
	if err != nil {
		t.Fatal(err)
	}
	state := maintenanceState{Version: 1, ActiveRequest: "accepted", LatestRequest: "accepted"}
	if err = writeMaintenanceJSON(directory, "state.json", state); err != nil {
		t.Fatal(err)
	}
	unlock()
	if receipt, err := maintenanceStatus(p, "accepted"); err == nil || receipt != nil {
		t.Fatalf("missing accepted receipt became unaccepted: %+v %v", receipt, err)
	}
	if receipt, err := maintenanceStatus(p, "never-accepted"); err != nil || receipt != nil {
		t.Fatalf("unknown request: %+v %v", receipt, err)
	}
}

func TestReplacementKeepsDaemonIdentityAfterFailedResetRemovedServiceData(t *testing.T) {
	p, old, a := maintenanceFixture(t)
	p.service.Mode = serviceModeDaemon
	p.service.User = "original-service"
	p.service.DisablePairing = true
	old.DataPolicy = "reset"
	old.ConfirmDataDeletion = true
	a.restore = func(paths, string) error { return errors.New("restore failed after reset") }
	if _, err := runMaintenance(p, old, a); err == nil {
		t.Fatal("expected failure")
	}
	next := old
	next.RequestID = "next"
	next.SupersedesRequestID = old.RequestID
	owner, err := maintenanceServiceReceipt(p, next)
	if err != nil || owner.ServiceMode != serviceModeDaemon || owner.ServiceUser != "original-service" || !owner.DisablePairing {
		t.Fatalf("lost original service owner: %+v %v", owner, err)
	}
}

func TestOrdinaryAdmissionWriteFailureRemainsUnacceptedAndRetriesExactScope(t *testing.T) {
	p, prior, a := maintenanceFixture(t)
	if _, err := runMaintenance(p, prior, a); err != nil {
		t.Fatal(err)
	}
	next := prior
	next.RequestID = "next"
	a.commitAdmission = func(*os.File, maintenanceState) error { return errors.New("admission commit failed") }
	a.stop = func(paths) error { t.Fatal("effect before admission"); return nil }
	if _, err := runMaintenance(p, next, a); err == nil {
		t.Fatal("expected write failure")
	}
	if got, err := maintenanceStatus(p, next.RequestID); got != nil || err != nil {
		t.Fatalf("unaccepted ordinary receipt: %+v %v", got, err)
	}
	a.commitAdmission = nil
	stops := 0
	a.stop = func(paths) error { stops++; return nil }
	got, err := runMaintenance(p, next, a)
	if err != nil || got.Outcome != "succeeded" || stops != 1 {
		t.Fatalf("%+v stops=%d %v", got, stops, err)
	}
}

func TestStagedOrdinaryReinstallCannotReviveAfterLaterUninstall(t *testing.T) {
	p, staged, a := maintenanceFixture(t)
	a.commitAdmission = func(*os.File, maintenanceState) error { return errors.New("admission commit failed") }
	if _, err := runMaintenance(p, staged, a); err == nil {
		t.Fatal("expected write failure")
	}
	uninstall := staged
	uninstall.RequestID = "later-uninstall"
	uninstall.Action = "uninstall"
	uninstall.SourceApp = ""
	uninstall.TargetReleaseID = ""
	uninstall.ArtifactSHA256 = ""
	uninstall.ArtifactSize = 0
	a.commitAdmission = nil
	if got, err := runMaintenance(p, uninstall, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("%+v %v", got, err)
	}
	a.stop = func(paths) error { t.Fatal("stale proposal repeated effects"); return nil }
	if _, err := runMaintenance(p, staged, a); err == nil {
		t.Fatal("stale staged reinstall revived after uninstall")
	}
}

func TestMissingAcceptedOrSupersededReceiptCannotBeRecreatedByReplay(t *testing.T) {
	for _, kind := range []string{"active", "superseded"} {
		t.Run(kind, func(t *testing.T) {
			p, r, a := maintenanceFixture(t)
			directory, unlock, err := lockMaintenance(p)
			if err != nil {
				t.Fatal(err)
			}
			state := maintenanceState{Version: 1, ActiveRequest: r.RequestID, LatestRequest: r.RequestID}
			if kind == "superseded" {
				state.ActiveRequest = ""
				state.LatestRequest = "later"
				state.SupersededRequests = map[string]string{r.RequestID: "later"}
			}
			if err = writeMaintenanceJSON(directory, "state.json", state); err != nil {
				t.Fatal(err)
			}
			unlock()
			a.stop = func(paths) error { t.Fatal("replayed an accepted request with lost evidence"); return nil }
			if _, err = runMaintenance(p, r, a); err == nil {
				t.Fatal("recreated missing accepted receipt")
			}
		})
	}
}

func TestStuckMaintenanceWithLiveExecutorReportsOwnerBusy(t *testing.T) {
	p, old, a := maintenanceFixture(t)
	stickMaintenance(t, p, old, a)
	directory, err := openMaintenanceDirectory(p, false)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	executor, err := openStateFileAt(directory, "owner.lock", unix.O_RDWR, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	if err := unix.Flock(int(executor.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	next := old
	next.RequestID, next.SupersedesRequestID, next.Action = "next", old.RequestID, "update"
	a.stop = func(paths) error { t.Fatal("replacement ran beside a live executor"); return nil }
	if _, err := runMaintenance(p, next, a); !errors.Is(err, errMaintenanceOwnerBusy) || !strings.HasPrefix(err.Error(), "owner_busy:") {
		t.Fatalf("err = %v", err)
	}
	unix.Flock(int(executor.Fd()), unix.LOCK_UN)
	if got, err := maintenanceStatus(p, old.RequestID); err != nil || got.Outcome != "pending" || got.SupersededByRequestID != "" {
		t.Fatalf("old = %+v %v", got, err)
	}
	if got, err := maintenanceStatus(p, next.RequestID); err != nil || got != nil {
		t.Fatalf("next = %+v %v", got, err)
	}
}

func TestRefusedPreserveReplacementKeepsStuckRequestForResetReplacement(t *testing.T) {
	p, old, a := maintenanceFixture(t)
	stickMaintenance(t, p, old, a)
	next := old
	next.RequestID, next.SupersedesRequestID, next.Action = "next", old.RequestID, "update"
	a.dataSchema = func(paths) (int, error) { return 35, nil }
	a.targetSchema = func(maintenanceRequest) (store.SchemaRange, error) { return store.SchemaRange{Min: 18, Max: 25}, nil }
	a.stop = func(paths) error { t.Fatal("refused replacement made an effect"); return nil }
	if got, err := runMaintenance(p, next, a); err == nil || got.FailureCode != "target_older_than_data" {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err := maintenanceStatus(p, old.RequestID); err != nil || got.Outcome != "pending" {
		t.Fatalf("refusal superseded the old request: %+v %v", got, err)
	}
	reset := old
	reset.RequestID, reset.SupersedesRequestID = "reset", old.RequestID
	reset.DataPolicy, reset.ConfirmDataDeletion = "reset", true
	a.stop = func(paths) error { return nil }
	if got, err := runMaintenance(p, reset, a); err != nil || got.Outcome != "succeeded" {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err := maintenanceStatus(p, old.RequestID); err != nil || got.SupersededByRequestID != reset.RequestID {
		t.Fatalf("old = %+v %v", got, err)
	}
}
