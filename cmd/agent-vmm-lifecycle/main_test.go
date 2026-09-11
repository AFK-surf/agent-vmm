package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseFuserOwners(t *testing.T) {
	if got, want := parseFuserOwners([]byte("4044 4051\n")), []int{4044, 4051}; !reflect.DeepEqual(got, want) {
		t.Fatalf("owners=%v want=%v", got, want)
	}
	if got := parseFuserOwners(nil); len(got) != 0 {
		t.Fatalf("empty output returned owners=%v", got)
	}
}

func TestFuserOpenOwnersTreatsUnownedFileAsReleasedOnMacOS(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("the lifecycle helper uses the macOS fuser contract")
	}
	path := filepath.Join(t.TempDir(), "state.raw")
	if err := os.WriteFile(path, []byte("state"), 0o600); err != nil {
		t.Fatal(err)
	}
	owners, err := fuserOpenOwners(context.Background(), path)
	if err != nil || len(owners) != 0 {
		t.Fatalf("unowned state disk owners=%v err=%v", owners, err)
	}
}

func TestXMLEscape(t *testing.T) {
	if got := xmlEscape("/Users/a&b<c>"); got != "/Users/a&amp;b&lt;c&gt;" {
		t.Fatalf("xmlEscape() = %q", got)
	}
}

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

func testServiceIdentity(t *testing.T) (int, int) {
	t.Helper()
	if os.Geteuid() == 0 {
		return 65534, 65534
	}
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, gid := range groups {
		if gid != os.Getegid() {
			return os.Geteuid(), gid
		}
	}
	return os.Geteuid(), os.Getegid()
}

func assertFileOwner(t *testing.T, path string, uid, gid int) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if int(stat.Uid) != uid || int(stat.Gid) != gid {
		t.Fatalf("state %s owner = %d:%d, want %d:%d", path, stat.Uid, stat.Gid, uid, gid)
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

func TestLifecycleOperationRejectsAncestorSymlinkWithoutChangingTarget(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	value.service.Mode = serviceModeDaemon
	value.service.UID, value.service.GID = testServiceIdentity(t)
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	victim := filepath.Join(root, "victim-library")
	if err := os.Mkdir(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	beforeStat := before.Sys().(*syscall.Stat_t)
	if err := os.Symlink(victim, filepath.Join(value.home, "Library")); err != nil {
		t.Fatal(err)
	}
	operation := lifecycleOperation{OperationID: "operation-ancestor", RequestID: "request-ancestor", Kind: "repair", Outcome: "pending"}
	if err := persistOperationOwned(value, operation, owner); err == nil {
		t.Fatal("symlinked service-state ancestor was accepted")
	}
	after, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	afterStat := after.Sys().(*syscall.Stat_t)
	if beforeStat.Uid != afterStat.Uid || beforeStat.Gid != afterStat.Gid || before.Mode().Perm() != after.Mode().Perm() {
		t.Fatalf("ancestor symlink target metadata changed: before=%d:%d/%o after=%d:%d/%o", beforeStat.Uid, beforeStat.Gid, before.Mode().Perm(), afterStat.Uid, afterStat.Gid, after.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(victim, "Application Support")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ancestor symlink target contents changed: %v", err)
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

func TestInstallOperationAcknowledgementCleansOnlyTheBoundOperation(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-terminal"
	operationDirectory := filepath.Join(value.applicationData, "install-operations", operationID)
	otherDirectory := filepath.Join(value.applicationData, "install-operations", "operation-other")
	for _, directory := range []string{operationDirectory, otherDirectory} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, "plan.json"), []byte("secret"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	parsed, err := parseInstallOperationAckArgs([]string{"--operation-id", operationID})
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupInstallPlan(value, parsed); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(operationDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged operation plan was retained: %v", err)
	}
	if _, err := os.Stat(otherDirectory); err != nil {
		t.Fatalf("another operation was removed: %v", err)
	}
	if err := cleanupInstallPlan(value, parsed); err != nil {
		t.Fatalf("repeated acknowledgement was not idempotent: %v", err)
	}
}

func TestInstallOperationAcknowledgementRejectsUnboundPath(t *testing.T) {
	value := testPaths(t.TempDir())
	otherDirectory := filepath.Join(value.applicationData, "install-operations", "operation-other")
	if err := os.MkdirAll(otherDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := parseInstallOperationAckArgs([]string{"--operation-id", "../operation-other"}); err == nil {
		t.Fatal("path-like operation id was accepted")
	}
	if err := cleanupInstallPlan(value, "../operation-other"); err == nil {
		t.Fatal("path-like operation id reached cleanup")
	}
	if _, err := os.Stat(otherDirectory); err != nil {
		t.Fatalf("rejected operation id changed another operation: %v", err)
	}
}

func TestAtomicReplaceAppSwapsCompleteBundles(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "Agent VMM Host.app")
	source := filepath.Join(parent, ".Agent VMM Host.staged.app")
	for path, value := range map[string]string{target: "old", source: "new"} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "release"), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := atomicReplaceApp(source, target); err != nil {
		t.Fatal(err)
	}
	installed, err := os.ReadFile(filepath.Join(target, "release"))
	if err != nil || string(installed) != "new" {
		t.Fatalf("installed bundle = %q, %v", installed, err)
	}
	retired, err := os.ReadFile(filepath.Join(source, "release"))
	if err != nil || string(retired) != "old" {
		t.Fatalf("retired bundle = %q, %v", retired, err)
	}
}

func TestLifecyclePathsRejectsTheDebugAppBundle(t *testing.T) {
	root := t.TempDir()
	_, err := lifecyclePathsFor(filepath.Join(root, "home"), filepath.Join(root, "Agent VMM.app", "Contents", "Helpers", "agent-vmm-lifecycle"))
	if err == nil || !strings.Contains(err.Error(), "Agent VMM Host.app") {
		t.Fatalf("debug app was accepted: %v", err)
	}
}

func TestManagedRepairArgumentIsIdempotent(t *testing.T) {
	input := "<array>\n<string>agent-vmm-host</string>\n</array>"
	once := addProgramArgument(input, "--disable-personal-mesh-pairing")
	twice := addProgramArgument(once, "--disable-personal-mesh-pairing")
	if once != twice || strings.Count(once, "--disable-personal-mesh-pairing") != 1 {
		t.Fatalf("managed argument render is not idempotent: %q", twice)
	}
}

func TestRepairInstallsDefinitionsBeforeLoadingServices(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	template := []byte("<plist><string>__HOME__</string><string>__APP__</string></plist>")
	for _, name := range []string{hostTemplateResource} {
		if err := os.WriteFile(filepath.Join(value.resources, name), template, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	if err := repairWith(value, func(arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		if arguments[0] == "print" {
			return errLaunchctlServiceNotFound
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !regular(value.hostPlist) {
		t.Fatal("service definitions were not installed")
	}
	want := []string{"bootout", "print", "bootout", "print", "bootstrap", "enable"}
	if len(commands) != len(want) {
		t.Fatalf("commands=%v", commands)
	}
	for index, prefix := range want {
		if !strings.HasPrefix(commands[index], prefix) {
			t.Fatalf("commands[%d]=%q", index, commands[index])
		}
	}
}

func TestRepairWaitsForBootoutToConvergeBeforeBootstrap(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte("<plist/>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	prints := 0
	err := repairWith(value, func(arguments ...string) error {
		commands = append(commands, arguments[0])
		if arguments[0] == "print" {
			prints++
			if prints >= 3 {
				return errLaunchctlServiceNotFound
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(commands, ","); got != "bootout,print,print,print,bootout,print,bootstrap,enable" {
		t.Fatalf("launchctl order=%s", got)
	}
}

func TestRepairFailsWhenBootoutFailsAndTheOldServiceRemainsLoaded(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte("<plist/>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	err := repairWith(value, func(arguments ...string) error {
		commands = append(commands, arguments[0])
		if arguments[0] == "bootout" {
			return errors.New("operation already in progress")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "remains loaded") {
		t.Fatalf("repair error=%v", err)
	}
	if got := strings.Join(commands, ","); got != "bootout,print" {
		t.Fatalf("launchctl order=%s", got)
	}
}

func TestUninstallKeepsDataUnlessPurgeWasConfirmed(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run(map[bool]string{false: "keep", true: "purge"}[purge], func(t *testing.T) {
			value := testPaths(t.TempDir())
			for _, directory := range []string{value.app, filepath.Dir(value.hostPlist), value.applicationData, value.logs} {
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, plist := range []string{value.hostPlist} {
				if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var commands []string
			if err := uninstallWith(value, purge, func(arguments ...string) error {
				commands = append(commands, strings.Join(arguments, " "))
				if arguments[0] == "print" {
					return errLaunchctlServiceNotFound
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if directory(value.app) || regular(value.hostPlist) {
				t.Fatal("installed product files remain")
			}
			if directory(value.applicationData) == purge || directory(value.logs) == purge {
				t.Fatalf("purge=%v data existence is wrong", purge)
			}
			if len(commands) != 2 || !strings.HasPrefix(commands[0], "bootout") || !strings.HasPrefix(commands[1], "print") {
				t.Fatalf("commands=%v", commands)
			}
		})
	}
}

func TestUninstallKeepsFilesWhenTheRunningServiceCannotBeStopped(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, []byte("plist"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := uninstallWith(value, false, func(arguments ...string) error {
		if arguments[0] == "bootout" {
			return errors.New("operation already in progress")
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "remains loaded") {
		t.Fatalf("uninstall error=%v", err)
	}
	if !directory(value.app) || !regular(value.hostPlist) {
		t.Fatal("uninstall removed files while the host service was still loaded")
	}
}

func TestUninstallKeepsFilesWhenServiceStatusCannotBeRead(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(value.hostPlist, []byte("plist"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := uninstallWith(value, false, func(arguments ...string) error {
		return errors.New("launchd communication failed")
	})
	if err == nil || !strings.Contains(err.Error(), "status could not be read") {
		t.Fatalf("uninstall error=%v", err)
	}
	if !directory(value.app) || !regular(value.hostPlist) {
		t.Fatal("uninstall removed files without proving the host service stopped")
	}
}

func TestStopHostServicePreservesDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := stopHostService(ctx, func(_ context.Context, _ ...string) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop error=%v", err)
	}
}

func TestLaunchctlServiceAbsentClassifiesHeadlessGUIDomain(t *testing.T) {
	for _, message := range []string{
		`Could not find service "com.afk-surf.agent-vmm-host" in domain for system`,
		"Could not find domain for user",
		"Could not print domain: 125: Domain does not support specified action",
	} {
		if !launchctlServiceAbsent("print", message) {
			t.Fatalf("expected absent service classification for %q", message)
		}
	}
	if launchctlServiceAbsent("bootstrap", "Domain does not support specified action") {
		t.Fatal("only print may prove that a service is absent")
	}
}

func TestParseLaunchctlServicePIDRequiresAnExplicitState(t *testing.T) {
	pid, err := parseLaunchctlServicePID([]byte("state = running\n\tpid = 42\n"))
	if err != nil || pid != 42 {
		t.Fatalf("running service pid=%d err=%v", pid, err)
	}
	pid, err = parseLaunchctlServicePID([]byte("state = not running\n"))
	if err != nil || pid != 0 {
		t.Fatalf("stopped service pid=%d err=%v", pid, err)
	}
	if _, err := parseLaunchctlServicePID([]byte("state = running\n")); err == nil {
		t.Fatal("running service without a PID was accepted as stopped")
	}
	if _, err := parseLaunchctlServicePID([]byte("state = not running\n\tpid = invalid\n")); err == nil {
		t.Fatal("stopped service with an invalid PID was accepted")
	}
}

func TestRepairPreservesContextCancellationDuringLaunchctlSteps(t *testing.T) {
	for _, interrupted := range []string{"bootstrap", "enable"} {
		t.Run(interrupted, func(t *testing.T) {
			value := testPaths(t.TempDir())
			if err := os.MkdirAll(value.resources, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(value.resources, hostTemplateResource), []byte("<plist/>"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(value.app, 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			err := repairWithOptionsContext(ctx, value, func(ctx context.Context, arguments ...string) error {
				switch arguments[0] {
				case "bootout":
					return errLaunchctlServiceNotFound
				case "print":
					return errLaunchctlServiceNotFound
				case interrupted:
					<-ctx.Done()
					return errors.New("launchctl was killed")
				default:
					return nil
				}
			}, false)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("repair error=%v", err)
			}
		})
	}
}

func TestDrainStopsHost(t *testing.T) {
	value := testPaths(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(value.hostPlist), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, plist := range []string{value.hostPlist} {
		if err := os.WriteFile(plist, []byte("plist"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var commands []string
	if err := drainWith(value, func(arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		if arguments[0] == "print" {
			return errLaunchctlServiceNotFound
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 6 {
		t.Fatalf("drain order=%v", commands)
	}
	for _, domain := range []string{value.serviceDomain(), value.alternateServiceDomain()} {
		foundBootout := false
		for _, command := range commands {
			foundBootout = foundBootout || strings.HasPrefix(command, "bootout "+domain+"/")
		}
		if !foundBootout {
			t.Fatalf("drain did not stop domain %s: %v", domain, commands)
		}
	}
	if !strings.HasPrefix(commands[0], "print") || !strings.HasPrefix(commands[1], "print") {
		t.Fatalf("drain order=%v", commands)
	}
}

func TestDrainWaitsForHostPIDAfterLaunchdServiceDisappears(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalPID, originalRunning := drainVMForUpdate, launchctlPID, processRunning
	t.Cleanup(func() { drainVMForUpdate, launchctlPID, processRunning = originalDrain, originalPID, originalRunning })
	drainVMForUpdate = func(context.Context, paths, string) error { return nil }
	launchctlPID = func(context.Context, string) (int, error) { return 42, nil }
	checks := 0
	processRunning = func(pid int) bool {
		if pid != 42 {
			t.Fatalf("pid=%d", pid)
		}
		checks++
		return checks < 3
	}
	prints := 0
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" {
			prints++
			if prints > 2 {
				return errLaunchctlServiceNotFound
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checks < 3 {
		t.Fatal("drain returned when launchd print disappeared but the Host PID was still alive")
	}
}

func TestDrainReplacesLoadedStoppedHostWhenOperatorIsUnavailable(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalPID, originalOwners := drainVMForUpdate, launchctlPID, stateDiskOpenOwners
	t.Cleanup(func() {
		drainVMForUpdate, launchctlPID, stateDiskOpenOwners = originalDrain, originalPID, originalOwners
	})
	drainVMForUpdate = func(context.Context, paths, string) error {
		return errors.New("stale operator socket")
	}
	launchctlPID = func(context.Context, string) (int, error) { return 0, nil }
	stateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	var commands []string
	bootedOut := false
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		if arguments[0] == "bootout" {
			bootedOut = true
		}
		if arguments[0] == "print" && bootedOut {
			return errLaunchctlServiceNotFound
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bootedOut {
		t.Fatalf("drain did not remove the stopped launchd job: %v", commands)
	}
}

func TestDrainPreservesRunningHostWhenOperatorIsUnavailable(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalPID := drainVMForUpdate, launchctlPID
	t.Cleanup(func() { drainVMForUpdate, launchctlPID = originalDrain, originalPID })
	drainVMForUpdate = func(context.Context, paths, string) error {
		return errors.New("stale operator socket")
	}
	launchctlPID = func(context.Context, string) (int, error) { return 42, nil }
	bootedOut := false
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		bootedOut = bootedOut || arguments[0] == "bootout"
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "stale operator socket") {
		t.Fatalf("drain error=%v", err)
	}
	if bootedOut {
		t.Fatal("drain removed the running Host after VM status failed")
	}
}

func TestDrainPreservesHostWhenPIDLookupRacesWithLaunchdRemoval(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalPID := drainVMForUpdate, launchctlPID
	t.Cleanup(func() { drainVMForUpdate, launchctlPID = originalDrain, originalPID })
	drainVMForUpdate = func(context.Context, paths, string) error {
		t.Fatal("VM drain ran without a reliable Host process observation")
		return nil
	}
	launchctlPID = func(context.Context, string) (int, error) {
		return 0, errLaunchctlServiceNotFound
	}
	bootedOut := false
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		bootedOut = bootedOut || arguments[0] == "bootout"
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "process identity could not be read") {
		t.Fatalf("drain error=%v", err)
	}
	if bootedOut {
		t.Fatal("drain removed the Host after its PID lookup became unavailable")
	}
}

func TestUpdateDrainRequestIDsAreUnique(t *testing.T) {
	first := newOperationID("update-drain")
	second := newOperationID("update-drain")
	if first == second {
		t.Fatalf("retry reused request id %q", first)
	}
}

type updateVMClientStub struct {
	state        hostv1.VMState
	stopRequests []*hostv1.StopVMRequest
}

func (client *updateVMClientStub) GetVM(context.Context, *hostv1.GetVMRequest, ...grpc.CallOption) (*hostv1.VMResponse, error) {
	return &hostv1.VMResponse{Vm: &hostv1.VM{State: client.state}}, nil
}

func (client *updateVMClientStub) StopVM(_ context.Context, request *hostv1.StopVMRequest, _ ...grpc.CallOption) (*hostv1.OperationResponse, error) {
	client.stopRequests = append(client.stopRequests, request)
	if !request.GetForce() {
		return nil, errors.New("graceful deadline")
	}
	client.state = hostv1.VMState_VM_STATE_STOPPED
	return &hostv1.OperationResponse{}, nil
}

func TestUpdateDrainForcesAndConfirmsTerminalAfterGracefulFailure(t *testing.T) {
	client := &updateVMClientStub{state: hostv1.VMState_VM_STATE_FAILED}
	if err := stopVMForUpdateWith(context.Background(), client, "update-attempt"); err != nil {
		t.Fatal(err)
	}
	if len(client.stopRequests) != 2 || client.stopRequests[0].GetForce() || !client.stopRequests[1].GetForce() {
		t.Fatalf("stop requests=%+v", client.stopRequests)
	}
	if client.stopRequests[0].GetRequestId() == client.stopRequests[1].GetRequestId() {
		t.Fatal("graceful and force stop reused one durable request")
	}
}

func TestDrainStopsReachableOrphanHostWithoutLaunchdJob(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalOwners := drainVMForUpdate, stateDiskOpenOwners
	t.Cleanup(func() { drainVMForUpdate, stateDiskOpenOwners = originalDrain, originalOwners })
	stopCalls := 0
	drainVMForUpdate = func(context.Context, paths, string) error {
		stopCalls++
		return nil
	}
	stateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" {
			return errLaunchctlServiceNotFound
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopCalls != 1 {
		t.Fatalf("orphan Host stop attempts=%d", stopCalls)
	}
}

func TestDrainWaitsForExactStateDiskOwnerAfterJobsDisappear(t *testing.T) {
	value := testPaths(t.TempDir())
	originalDrain, originalOwners := drainVMForUpdate, stateDiskOpenOwners
	t.Cleanup(func() { drainVMForUpdate, stateDiskOpenOwners = originalDrain, originalOwners })
	drainVMForUpdate = func(context.Context, paths, string) error { return errors.New("stale operator socket") }
	checks := 0
	stateDiskOpenOwners = func(_ context.Context, path string) ([]int, error) {
		want := filepath.Join(value.applicationData, "instance", "state.raw")
		if path != want {
			t.Fatalf("owner check path=%q want=%q", path, want)
		}
		checks++
		if checks < 3 {
			return []int{4242}, nil
		}
		return nil, nil
	}
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" {
			return errLaunchctlServiceNotFound
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checks < 3 {
		t.Fatal("drain did not wait for the exact state.raw owner to release")
	}
}

func TestSetRemoteRegistrationStateUsesExactRegistrationAndRevision(t *testing.T) {
	registration := &hostv1.RemoteRegistration{
		Id:       "registration-a",
		Revision: 7,
		State:    hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED,
	}
	var request *hostv1.SetRemoteRegistrationStateRequest
	err := setRemoteRegistrationStateWith(
		context.Background(),
		"registration-a",
		hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING,
		"request-a",
		func(_ context.Context, input *hostv1.GetRemoteRegistrationRequest, _ ...grpc.CallOption) (*hostv1.RemoteRegistrationResponse, error) {
			if input.GetRegistrationId() != "registration-a" {
				t.Fatalf("get registration=%q", input.GetRegistrationId())
			}
			return &hostv1.RemoteRegistrationResponse{Registration: registration}, nil
		},
		func(_ context.Context, input *hostv1.SetRemoteRegistrationStateRequest, _ ...grpc.CallOption) (*hostv1.RemoteRegistrationResponse, error) {
			request = input
			return &hostv1.RemoteRegistrationResponse{Registration: registration}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if request.GetRegistrationId() != "registration-a" || request.GetExpectedRevision() != 7 || request.GetRequestId() != "request-a" || request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING {
		t.Fatalf("state request=%+v", request)
	}
}

func TestLifecycleOperationIsDurableAndRequestIdempotent(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runLifecycleOperation(value, "install", "request-1", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Outcome != "succeeded" || calls != 1 {
		t.Fatalf("first operation=%+v calls=%d", first, calls)
	}
	second, err := runLifecycleOperation(value, "install", "request-1", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.OperationID != first.OperationID || calls != 1 {
		t.Fatalf("duplicate operation=%+v calls=%d", second, calls)
	}
	observed := readOperation(value.operationFile)
	if observed == nil || observed.OperationID != first.OperationID || observed.Outcome != "succeeded" {
		t.Fatalf("durable operation=%+v", observed)
	}
}

func TestLifecycleOperationFailureIsRecorded(t *testing.T) {
	value := testPaths(t.TempDir())
	_, err := runLifecycleOperation(value, "remove", "request-2", func() error { return os.ErrPermission })
	if err == nil {
		t.Fatal("expected operation failure")
	}
	observed := readOperation(value.operationFile)
	if observed == nil || observed.Outcome != "failed" || observed.Kind != "remove" {
		t.Fatalf("failed operation=%+v", observed)
	}
}

func TestManagedLifecycleOperationRetriesFailedStableRequest(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runManagedLifecycleOperation(value, "install-managed", "operation-1", func() error {
		calls++
		return context.DeadlineExceeded
	})
	if err == nil || first.Outcome != "failed" {
		t.Fatalf("first managed operation=%+v err=%v", first, err)
	}
	second, err := runManagedLifecycleOperation(value, "install-managed", "operation-1", func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || second.Outcome != "succeeded" || second.OperationID != "operation-1" ||
		second.OperationID != first.OperationID || second.RequestID != first.RequestID {
		t.Fatalf("retried managed operation first=%+v second=%+v calls=%d", first, second, calls)
	}
}

func TestManagedLifecycleOperationReportsPermanentFailureWithoutRetry(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runManagedLifecycleOperation(value, "install-managed", "operation-terminal", func() error {
		calls++
		return os.ErrPermission
	})
	if err == nil || first.Retryable || first.FailureCode != "install_failed" {
		t.Fatalf("terminal managed operation=%+v err=%v", first, err)
	}
	if first.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("durable non-retryable error did not enter terminal reporting: %+v", first)
	}
	second, err := runManagedLifecycleOperation(value, "install-managed", "operation-terminal", func() error {
		calls++
		return nil
	})
	if err == nil || calls != 1 || second.OperationID != first.OperationID || second.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("terminal operation was retried: first=%+v second=%+v calls=%d err=%v", first, second, calls, err)
	}
}

func TestTerminalManagedLifecycleFailureRetainsPlanForServerAcknowledgement(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-terminal-cleanup"
	planDirectory := filepath.Join(value.applicationData, "install-operations", operationID)
	if err := os.MkdirAll(planDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(planDirectory, "plan.json"), []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		return terminalManagedInstallError(os.ErrPermission)
	})
	if operationErr == nil || operation.Retryable {
		t.Fatalf("permanent failure was not terminal: operation=%+v err=%v", operation, operationErr)
	}
	if operation.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("permanent failure did not request terminal reporting: %+v", operation)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("terminal operation error was lost")
	}
	if _, err := os.Stat(planDirectory); err != nil {
		t.Fatalf("terminal failure removed plan before Server acknowledgement: %v", err)
	}
	recorded, err := os.ReadFile(value.operationFile)
	if err != nil || strings.Contains(string(recorded), "secret-token") || !strings.Contains(string(recorded), `"outcome":"failed"`) {
		t.Fatalf("non-secret terminal operation record missing or leaked material: %s err=%v", recorded, err)
	}
}

func TestRetryableManagedLifecycleFailureRetainsPlan(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-retryable-plan"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		return context.DeadlineExceeded
	})
	if operationErr == nil || !operation.Retryable {
		t.Fatalf("transient failure was not retryable: operation=%+v err=%v", operation, operationErr)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("retryable operation error was lost")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("retryable failure removed durable plan: %v", err)
	}
}

func TestNonRetryableManagedFailureRetainsPlanUntilTerminalReportAck(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-storage-failure"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		return &os.PathError{Op: "sync", Path: planPath, Err: os.ErrPermission}
	})
	if operationErr == nil || operation.Retryable || operation.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("non-retryable failure did not enter terminal reporting: operation=%+v err=%v", operation, operationErr)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("storage failure error was lost")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("storage failure removed durable plan: %v", err)
	}
	if err := cleanupInstallPlan(value, operationID); err != nil {
		t.Fatalf("acknowledged terminal report did not clean the retained plan: %v", err)
	}
	if _, err := os.Stat(planPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged terminal plan remains: %v", err)
	}
}

func TestNonRetryableResultRecordFailureRetainsPlan(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-record-failure"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(value.applicationData, 0o700) })
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		if err := os.Chmod(value.applicationData, 0o500); err != nil {
			t.Fatal(err)
		}
		return os.ErrPermission
	})
	if err := os.Chmod(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	if operationErr == nil || operation.SecretDisposition != secretDispositionRetain {
		t.Fatalf("undurable terminal result did not retain its plan: operation=%+v err=%v", operation, operationErr)
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("record failure removed retained plan: %v", err)
	}
}

func TestManagedLifecycleLockContentionRetainsPlan(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-lock-contention"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	release, err := acquireOperationLock(value)
	if err != nil {
		t.Fatal(err)
	}
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		t.Fatal("lock loser must not start the managed install")
		return nil
	})
	release()
	if operationErr == nil || operation.SecretDisposition != secretDispositionRetain {
		t.Fatalf("lock contention disposition=%+v err=%v", operation, operationErr)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("lock contention error was lost")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("lock loser removed the operation owner's plan: %v", err)
	}
}

func TestSucceededManagedOperationWithoutCompletionDispositionRetainsPlan(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-corrupt-success"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := lifecycleOperation{
		OperationID: operationID, RequestID: operationID, Kind: "install-managed",
		Outcome: "succeeded", Attempt: 1, StartedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	if err := persistOperation(value, previous); err != nil {
		t.Fatal(err)
	}
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", operationID, func() error {
		t.Fatal("a recorded success must not replay the action")
		return nil
	})
	if operationErr != nil || operation.SecretDisposition != "" {
		t.Fatalf("corrupt success inferred disposition: operation=%+v err=%v", operation, operationErr)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("missing disposition was not diagnosed")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("missing disposition removed retained plan: %v", err)
	}
}

func TestDestroyCompletedRequiresDurableCompletionReceipt(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-missing-receipt"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operation := lifecycleOperation{SecretDisposition: secretDispositionDestroyCompleted}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, nil); err == nil {
		t.Fatal("destroy_completed without a receipt was accepted")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("missing completion receipt removed retained plan: %v", err)
	}
}

func TestDestroyTerminalRequiresDurableTerminalResult(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-missing-terminal-result"
	planPath := filepath.Join(value.applicationData, "install-operations", operationID, "plan.json")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"material":"secret-token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operation := lifecycleOperation{SecretDisposition: secretDispositionDestroyTerminal}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, os.ErrPermission); err == nil {
		t.Fatal("destroy_terminal without a durable result was accepted")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("missing terminal result removed retained plan: %v", err)
	}
}

func TestManagedLifecycleOperationStopsAtRetryLimit(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	fail := func() error {
		calls++
		return context.DeadlineExceeded
	}
	for attempt := 0; attempt < boundedOperationMaxAttempts; attempt++ {
		if _, err := runManagedLifecycleOperation(value, "install-managed", "operation-bounded", fail); err == nil {
			t.Fatalf("attempt %d unexpectedly succeeded", attempt+1)
		}
	}
	operation, err := runManagedLifecycleOperation(value, "install-managed", "operation-bounded", func() error {
		calls++
		return nil
	})
	if err == nil || calls != boundedOperationMaxAttempts || operation.Retryable || operation.FailureCode != "retry_limit_reached" {
		t.Fatalf("retry limit operation=%+v calls=%d err=%v", operation, calls, err)
	}
}

func TestManagedLifecycleOperationRetriesUnknownSameRequest(t *testing.T) {
	value := testPaths(t.TempDir())
	previous := lifecycleOperation{
		OperationID: "operation-unknown", RequestID: "operation-unknown", Kind: "install-managed",
		Outcome: "pending", Attempt: 1, StartedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	if err := persistOperation(value, previous); err != nil {
		t.Fatal(err)
	}
	calls := 0
	operation, err := runManagedLifecycleOperation(value, "install-managed", previous.RequestID, func() error {
		calls++
		return nil
	})
	if err != nil || calls != 1 || operation.OperationID != previous.OperationID || operation.Attempt != 2 || operation.Outcome != "succeeded" {
		t.Fatalf("unknown managed operation did not resume exactly: operation=%+v calls=%d err=%v", operation, calls, err)
	}
}

func TestLifecycleRequestIDFencesGeneratedVMStart(t *testing.T) {
	generated := lifecycleRequestID("")
	if generated == "" || !strings.HasPrefix(generated, "req-") {
		t.Fatalf("generated request ID = %q", generated)
	}
	if got := lifecycleRequestID("operator-request"); got != "operator-request" {
		t.Fatalf("explicit request ID changed to %q", got)
	}
}

func TestRegistrationStateOperationRetriesTransientSameRevision(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runRegistrationStateOperation(value, "registration-ENABLED", "operation-1-r2", func() error {
		calls++
		return status.Error(codes.Unavailable, "operator restarting")
	})
	if err == nil || !first.Retryable {
		t.Fatalf("transient registration state failure=%+v err=%v", first, err)
	}
	second, err := runRegistrationStateOperation(value, "registration-ENABLED", "operation-1-r2", func() error {
		calls++
		return nil
	})
	if err != nil || calls != 2 || second.OperationID != first.OperationID || second.Outcome != "succeeded" {
		t.Fatalf("registration state retry drifted: first=%+v second=%+v calls=%d err=%v", first, second, calls, err)
	}
}

func TestLifecycleOperationDoesNotReplayAnUnconfirmedPendingRequest(t *testing.T) {
	value := testPaths(t.TempDir())
	first, err := runLifecycleOperation(value, "install", "request-3", func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	first.Outcome = "pending"
	if err := persistOperation(value, first); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = runLifecycleOperation(value, "install", "request-3", func() error {
		calls++
		return nil
	})
	if err == nil || calls != 0 {
		t.Fatalf("pending operation was replayed: err=%v calls=%d", err, calls)
	}
	if observed := readOperation(value.operationFile); observed == nil || observed.Outcome != "unknown" {
		t.Fatalf("pending operation outcome=%+v", observed)
	}
}

func TestLifecycleOperationSerializesDuplicateRequests(t *testing.T) {
	value := testPaths(t.TempDir())
	var mu sync.Mutex
	calls := 0
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := runLifecycleOperation(value, "install", "request-concurrent", func() error {
				mu.Lock()
				calls++
				mu.Unlock()
				<-start
				return nil
			})
			results <- err
		}()
	}
	// Let the first action finish; the second must observe its durable result.
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("duplicate request executed %d actions", calls)
	}
}

func testPaths(root string) paths {
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o700); err != nil {
		panic(err)
	}
	agentPlist := filepath.Join(home, "Library", "LaunchAgents", hostLabel+".plist")
	return paths{
		home: home, app: filepath.Join(root, "Agent VMM Host.app"), resources: filepath.Join(root, "resources"),
		hostPlist:       agentPlist,
		agentPlist:      agentPlist,
		daemonPlist:     filepath.Join(root, "LaunchDaemons", hostLabel+".plist"),
		service:         serviceConfig{Version: 1, Mode: serviceModeAgent, User: "test", Group: "staff", UID: os.Getuid(), GID: os.Getgid(), ReleaseID: releaseID},
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"), logs: filepath.Join(home, "Library", "Logs", "AgentVMM"),
		operationFile:     filepath.Join(home, "Library", "Application Support", "AgentVMM", "compute-node-operation.json"),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}
}

func TestAtomicWriteReplacesDefinition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.plist")
	if err := atomicWrite(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "second" {
		t.Fatalf("definition=%q err=%v", data, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestHostHealthDoesNotConfirmManagedInstall(t *testing.T) {
	if operationConfirmed("install-managed", serviceStatus{HostInstalled: true, HostLoaded: true, HostHealthy: true}) {
		t.Fatal("managed install recovery was confirmed without exact registration observation")
	}
}
