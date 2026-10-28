package lifecycle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/launchdservice"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestObservationPathsDoNotRequireCompleteBundle(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	executable := filepath.Join(root, "Agent VMM Host.app", "Contents", "Helpers", "missing-lifecycle-helper")
	value, err := observationPathsForConfig(home, executable, serviceConfig{Mode: serviceModeAgent}, "")
	if err != nil {
		t.Fatal(err)
	}
	if value.home != home || value.app != filepath.Join(root, "Agent VMM Host.app") {
		t.Fatalf("observation paths = %+v", value)
	}
}

// crashLoopLaunchctlPrint has the structure of a real launchctl print for a
// Host that exits at start: launchd reports "spawn scheduled" without a PID.
const crashLoopLaunchctlPrint = `gui/501/com.afk-surf.agent-vmm-host = {
	active count = 0
	path = /Users/test/Library/LaunchAgents/com.afk-surf.agent-vmm-host.plist
	type = LaunchAgent
	state = spawn scheduled

	program = /Users/test/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/MacOS/agent-vmm-host
	arguments = {
		/Users/test/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/MacOS/agent-vmm-host
		--database
		/Users/test/Library/Application Support/AgentVMM/host.sqlite
	}

	stdout path = /Users/test/Library/Logs/AgentVMM/host.stdout.log
	stderr path = /Users/test/Library/Logs/AgentVMM/host.stderr.log
	domain = gui/501 [100020]
	asid = 100020
	minimum runtime = 10
	exit timeout = 60
	runs = 57
	last exit code = 1

	semaphores = {
		successful exit => 0
	}

	resource coalition = {
		ID = 165749
		type = resource
		state = active
		active count = 1
		name = com.afk-surf.agent-vmm-host
	}

	spawn type = interactive (4)
	job state = exited
	properties = runatload | inferred program
}
`

func TestParseLaunchctlServiceObservationSeparatesLoadedFromRunning(t *testing.T) {
	t.Parallel()
	running, err := parseLaunchctlServiceObservation([]byte(fmt.Sprintf("state = running\npid = %d\nlast exit code = 0\nresource coalition = {\n\tstate = active\n}\n", os.Getpid())))
	if err != nil || !running.Loaded || !running.Running || running.PID != os.Getpid() {
		t.Fatalf("running facts = %+v, err=%v", running, err)
	}
	exited, err := parseLaunchctlServiceObservation([]byte("state = exited\nlast exit status = 78\nresource coalition = {\n\tstate = active\n}\n"))
	if err != nil || !exited.Loaded || exited.Running || exited.PID != 0 || exited.LastExitStatus == nil || *exited.LastExitStatus != 78 {
		t.Fatalf("exited facts = %+v, err=%v", exited, err)
	}
	crashLoop, err := parseLaunchctlServiceObservation([]byte(crashLoopLaunchctlPrint))
	if err != nil || !crashLoop.Loaded || crashLoop.Running || crashLoop.PID != 0 || crashLoop.LastExitStatus == nil || *crashLoop.LastExitStatus != 1 {
		t.Fatalf("spawn scheduled facts = %+v, err=%v", crashLoop, err)
	}
	if _, err := parseLaunchctlServiceObservation([]byte("state = running\n\tpid = invalid\n")); err == nil {
		t.Fatal("invalid Host PID was accepted")
	}
}

func TestStatusReportsCrashLoopingHostLoadedButNotRunning(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.observe = func(context.Context, string) (hostServiceFacts, error) {
		return parseLaunchctlServiceObservation([]byte(crashLoopLaunchctlPrint))
	}
	status := readStatusContext(context.Background(), value)
	if !status.HostObserved || !status.HostLoaded || status.HostRunning || status.HostPID != 0 || status.HostObservationIssue != "" ||
		status.HostLastExitStatus == nil || *status.HostLastExitStatus != 1 {
		t.Fatalf("crash-looping Host status = %+v", status)
	}
}

func TestStatusDoesNotTurnLaunchctlFailureIntoKnownStoppedState(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.observe = func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{}, errors.New("launchd query denied")
	}
	status := readStatusContext(context.Background(), value)
	if status.HostObserved || status.HostLoaded || status.HostRunning || status.HostObservationIssue == "" {
		t.Fatalf("status collapsed launchd query failure: %+v", status)
	}
}

func TestVersionAndStatusReportSchemaCompatibility(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	writeContractFixture(t, "version.json", versionReport())
	version, err := json.Marshal(versionReport())
	schema := store.CurrentSchema()
	if err != nil || !strings.Contains(string(version), fmt.Sprintf(`"currentSchema":%d`, schema)) {
		t.Fatalf("version = %s %v", version, err)
	}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.observe = func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true}, nil
	}
	if status := readStatusContext(context.Background(), value); status.HostStartFailure != nil || status.DataSchema != 0 || status.DataSchemaIssue != "" {
		t.Fatalf("status without a record or database = %+v", status)
	}
	record := `{"code":"STATE_SCHEMA_INCOMPATIBLE","dataSchema":36,"currentSchema":35}`
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.applicationData, store.StartFailureFile), []byte(record), 0o600); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(value.applicationData, "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL); INSERT INTO schema_migrations VALUES (35, 0)`)
	database.Close()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(readStatusContext(context.Background(), value))
	if err != nil || !strings.Contains(string(encoded), `"hostStartFailure":`+record) || !strings.Contains(string(encoded), `"dataSchema":35`) {
		t.Fatalf("status = %s %v", encoded, err)
	}
}

func TestAtomicReplaceAppSwapsCompleteBundles(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	root := t.TempDir()
	_, err := lifecyclePathsFor(filepath.Join(root, "home"), filepath.Join(root, "Agent VMM.app", "Contents", "Helpers", "agent-vmm-lifecycle"))
	if err == nil || !strings.Contains(err.Error(), "Agent VMM Host.app") {
		t.Fatalf("debug app was accepted: %v", err)
	}
}

func TestRepairInstallsDefinitionsBeforeLoadingServices(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(value.app, 0o700); err != nil {
		t.Fatal(err)
	}
	var commands []string
	if err := repairWith(value, func(arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		if arguments[0] == "print" {
			return errHostServiceNotFound
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
	t.Parallel()
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
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
				return errHostServiceNotFound
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
	t.Parallel()
	root := t.TempDir()
	value := testPaths(root)
	if err := os.MkdirAll(value.resources, 0o700); err != nil {
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
	t.Parallel()
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
					return errHostServiceNotFound
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if directory(value.app) || regular(value.hostPlist) {
				t.Fatal("installed product files remain")
			}
			if !directory(value.applicationData) || directory(value.logs) == purge {
				t.Fatalf("purge=%v data existence is wrong", purge)
			}
			if len(commands) != 2 || !strings.HasPrefix(commands[0], "bootout") || !strings.HasPrefix(commands[1], "print") {
				t.Fatalf("commands=%v", commands)
			}
		})
	}
}

func TestUninstallKeepsFilesWhenTheRunningServiceCannotBeStopped(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := stopHostService(ctx, func(_ context.Context, _ ...string) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop error=%v", err)
	}
}

func TestLaunchctlServiceAbsentClassifiesHeadlessGUIDomain(t *testing.T) {
	t.Parallel()
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

func TestRepairPreservesContextCancellationDuringLaunchctlSteps(t *testing.T) {
	t.Parallel()
	for _, interrupted := range []string{"bootstrap", "enable"} {
		t.Run(interrupted, func(t *testing.T) {
			value := testPaths(t.TempDir())
			if err := os.MkdirAll(value.resources, 0o700); err != nil {
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
					return errHostServiceNotFound
				case "print":
					return errHostServiceNotFound
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
	t.Parallel()
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
			return errHostServiceNotFound
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
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.drainVM = func(context.Context, paths, string) error { return nil }
	owner.system.ServicePID = func(context.Context, string) (int, error) { return 42, nil }
	checks := 0
	owner.system.ProcessRunning = func(pid int) bool {
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
				return errHostServiceNotFound
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
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.drainVM = func(context.Context, paths, string) error {
		return errors.New("stale operator socket")
	}
	owner.system.ServicePID = func(context.Context, string) (int, error) { return 0, nil }
	owner.system.StateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	var commands []string
	bootedOut := false
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		commands = append(commands, strings.Join(arguments, " "))
		if arguments[0] == "bootout" {
			bootedOut = true
		}
		if arguments[0] == "print" && bootedOut {
			return errHostServiceNotFound
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
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.drainVM = func(context.Context, paths, string) error {
		return errors.New("stale operator socket")
	}
	owner.system.ServicePID = func(context.Context, string) (int, error) { return 42, nil }
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
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.drainVM = func(context.Context, paths, string) error {
		t.Fatal("VM drain ran without a reliable Host process observation")
		return nil
	}
	owner.system.ServicePID = func(context.Context, string) (int, error) {
		return 0, errHostServiceNotFound
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

func TestServiceUserDaemonDrainStopsVMBeforeAdministratorAction(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	if os.Geteuid() == 0 {
		t.Skip("requires non-root test process")
	}
	called := false
	owner.drainVM = func(context.Context, paths, string) error { called = true; return nil }
	launchctlCalled := false
	value := testPaths(t.TempDir())
	value.owner = &owner
	value.service.Mode = serviceModeDaemon
	err := drainWithContext(context.Background(), value, func(context.Context, ...string) error { launchctlCalled = true; return nil })
	if err == nil || !strings.Contains(err.Error(), "administrator must run") || !called || !launchctlCalled {
		t.Fatalf("err=%v vm stop=%t launchctl=%t", err, called, launchctlCalled)
	}
}

func TestDrainStopsReachableOrphanHostWithoutLaunchdJob(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	stopCalls := 0
	owner.drainVM = func(context.Context, paths, string) error {
		stopCalls++
		return nil
	}
	owner.system.StateDiskOpenOwners = func(context.Context, string) ([]int, error) {
		if stopCalls == 0 {
			return []int{456}, nil
		}
		return nil, nil
	}
	err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" {
			return errHostServiceNotFound
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stopCalls != 1 {
		t.Fatalf("orphan Host stop attempts=%d", stopCalls)
	}
	owner.system.StateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	stopCalls = 0
	if err := drainWithContext(context.Background(), value, func(_ context.Context, arguments ...string) error {
		if arguments[0] == "print" {
			return errHostServiceNotFound
		}
		return nil
	}); err != nil || stopCalls != 0 {
		t.Fatalf("absent Host required VM stop: calls=%d error=%v", stopCalls, err)
	}
}

func TestDrainWaitsForExactStateDiskOwnerAfterJobsDisappear(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	value := testPaths(t.TempDir())
	value.owner = &owner
	owner.drainVM = func(context.Context, paths, string) error { return errors.New("stale operator socket") }
	checks := 0
	owner.system.StateDiskOpenOwners = func(_ context.Context, path string) ([]int, error) {
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
			return errHostServiceNotFound
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

func TestDaemonDrainContinuesOnlyAfterFixedJobIsAdministratorStopped(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	if os.Geteuid() == 0 {
		t.Skip("requires the service-user daemon branch")
	}
	value := testPaths(t.TempDir())
	value.owner = &owner
	value.service.Mode = serviceModeDaemon
	value.hostPlist = value.daemonPlist
	drainCalls := 0
	owner.drainVM = func(context.Context, paths, string) error {
		drainCalls++
		return nil
	}
	owner.system.StateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	loadedErr := drainWithContext(context.Background(), value, func(context.Context, ...string) error { return nil })
	if loadedErr == nil || !strings.Contains(loadedErr.Error(), "agent-vmm-service-executor stop") {
		t.Fatalf("loaded daemon drain error=%v", loadedErr)
	}
	if drainCalls != 1 {
		t.Fatalf("loaded daemon drain calls=%d", drainCalls)
	}
	absentErr := drainWithContext(context.Background(), value, func(context.Context, ...string) error { return errHostServiceNotFound })
	if absentErr != nil {
		t.Fatalf("administrator-stopped daemon blocked forward cutover: %v", absentErr)
	}
	if drainCalls != 1 {
		t.Fatalf("administrator-stopped daemon retried unavailable Host RPC: calls=%d", drainCalls)
	}
}

func TestSetRemoteRegistrationStateUsesExactRegistrationAndRevision(t *testing.T) {
	t.Parallel()
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
		func(_ context.Context, input *hostv1.GetRemoteRegistrationRequest, _ ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error) {
			if input.GetRegistrationId() != "registration-a" {
				t.Fatalf("get registration=%q", input.GetRegistrationId())
			}
			return &hostv1.GetRemoteRegistrationResponse{Registration: registration}, nil
		},
		func(_ context.Context, input *hostv1.SetRemoteRegistrationStateRequest, _ ...grpc.CallOption) (*hostv1.SetRemoteRegistrationStateResponse, error) {
			request = input
			return &hostv1.SetRemoteRegistrationStateResponse{Registration: registration}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if request.GetRegistrationId() != "registration-a" || request.GetExpectedRevision() != 7 || request.GetRequestId() != "request-a" || request.GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING {
		t.Fatalf("state request=%+v", request)
	}
}

func TestSetEnabledRegistrationReconcilesDisconnectedConnector(t *testing.T) {
	t.Parallel()
	registration := &hostv1.RemoteRegistration{Id: "registration-a", Revision: 9, State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, ConnectionState: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED}
	called := false
	err := setRemoteRegistrationStateWith(context.Background(), "registration-a", hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, "request-a",
		func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error) {
			return &hostv1.GetRemoteRegistrationResponse{Registration: registration}, nil
		},
		func(_ context.Context, request *hostv1.SetRemoteRegistrationStateRequest, _ ...grpc.CallOption) (*hostv1.SetRemoteRegistrationStateResponse, error) {
			called = true
			if request.GetExpectedRevision() != 9 || request.GetRequestId() != "request-a" {
				t.Fatalf("request=%+v", request)
			}
			return &hostv1.SetRemoteRegistrationStateResponse{Registration: registration}, nil
		},
	)
	if err != nil || !called {
		t.Fatalf("err=%v called=%t", err, called)
	}
}

func TestRepairWaitsForExactRegistrationConnection(t *testing.T) {
	t.Parallel()
	reads := 0
	err := waitRemoteRegistrationConnectedWith(context.Background(), "registration-a", func(_ context.Context, request *hostv1.GetRemoteRegistrationRequest, _ ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error) {
		reads++
		if request.GetRegistrationId() != "registration-a" {
			t.Fatalf("request=%+v", request)
		}
		state := hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED
		if reads == 2 {
			state = hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
		}
		return &hostv1.GetRemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: "registration-a", ConnectionState: state}}, nil
	}, func(context.Context) error { return nil })
	if err != nil || reads != 2 {
		t.Fatalf("reads=%d err=%v", reads, err)
	}
}

func TestLifecycleRootBoundaryOnlyAllowsObservation(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"install", "install-operation-ack", "repair", "registration-state", "drain", "update", "uninstall"} {
		if !lifecycleMutationCommand(command) {
			t.Fatalf("%s was not classified as a business mutation", command)
		}
	}
	for _, command := range []string{"version", "status", "operation"} {
		if lifecycleMutationCommand(command) {
			t.Fatalf("%s observation was classified as a mutation", command)
		}
	}
}

func TestUpdateApplicationUsesOneForwardCutoverSequence(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	source := filepath.Join(filepath.Dir(value.app), ".Agent VMM Host.prepared.app")
	var calls []string
	actions := updateApplicationActions{
		releaseID: func(path string) (string, error) {
			calls = append(calls, "release:"+filepath.Base(path))
			if path == value.app {
				return "release-old", nil
			}
			return "release-new", nil
		},
		drain: func(paths) error { calls = append(calls, "drain"); return nil },
		replace: func(source, target string) error {
			calls = append(calls, "replace:"+filepath.Base(source)+":"+filepath.Base(target))
			return nil
		},
		repair:  func(paths) error { calls = append(calls, "repair"); return nil },
		startVM: func(context.Context, paths, string) error { calls = append(calls, "start"); return nil },
		remove:  func(path string) error { calls = append(calls, "remove:"+filepath.Base(path)); return nil },
	}
	operation, err := runUpdateLifecycleOperation(value, "update-request", "release-new", func() error {
		return updateApplicationWith(value, source, "release-new", "update-request", actions)
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"release:Agent VMM Host.app",
		"drain",
		"replace:.Agent VMM Host.prepared.app:Agent VMM Host.app",
		"repair", "start", "remove:.Agent VMM Host.prepared.app",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%v want=%v", calls, want)
	}
	if operation.Outcome != "succeeded" || operation.Stage != "settled" {
		t.Fatalf("operation=%+v", operation)
	}
}

func TestUpdateFailureAfterReplacementRetainsSourceAndSameRequestRepairsForward(t *testing.T) {
	t.Parallel()
	for _, failRemoval := range []bool{false, true} {
		t.Run(fmt.Sprint(failRemoval), func(t *testing.T) {
			value := testPaths(t.TempDir())
			source := filepath.Join(filepath.Dir(value.app), ".Agent VMM Host.prepared.app")
			currentRelease := "release-old"
			replaceCalls, repairCalls, removeCalls := 0, 0, 0
			var restoreIDs []string
			actions := updateApplicationActions{
				releaseID: func(path string) (string, error) { return currentRelease, nil },
				drain:     func(paths) error { return nil },
				replace: func(string, string) error {
					replaceCalls++
					if replaceCalls == 1 {
						return errors.New("replacement unavailable")
					}
					currentRelease = "release-new"
					return nil
				},
				repair: func(paths) error { repairCalls++; return nil },
				startVM: func(_ context.Context, _ paths, requestID string) error {
					restoreIDs = append(restoreIDs, requestID)
					if len(restoreIDs) == 1 || requestID == restoreIDs[0] {
						return errors.New("Host startup failed")
					}
					return nil
				},
				remove: func(string) error {
					removeCalls++
					if failRemoval {
						return errors.New("retired bundle is busy")
					}
					return nil
				},
			}
			run := func() (lifecycleOperation, error) {
				return runUpdateLifecycleOperation(value, "update-retry", "release-new", func() error {
					return updateApplicationWith(value, source, "release-new", "update-retry", actions)
				})
			}
			first, err := run()
			if err == nil || first.Stage != "replace" || first.Attempt != 1 || !first.Retryable {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			second, err := run()
			if err == nil || second.Stage != "restore" || second.Attempt != 1 || !second.Retryable || removeCalls != 0 {
				t.Fatalf("second=%+v err=%v removals=%d", second, err, removeCalls)
			}
			third, err := run()
			if failRemoval {
				if err == nil || third.Stage != "settled" || third.Attempt != 1 || !third.Retryable {
					t.Fatalf("first removal: %+v %v", third, err)
				}
				fourth, err := run()
				if err == nil || fourth.Attempt != 2 || !fourth.Retryable {
					t.Fatalf("second removal: %+v %v", fourth, err)
				}
				fifth, err := run()
				if err == nil || fifth.Attempt != 3 || fifth.Retryable {
					t.Fatalf("third removal: %+v %v", fifth, err)
				}
				if repairCalls != 2 || removeCalls != 3 {
					t.Fatalf("settled repeated restore: repairs=%d removals=%d", repairCalls, removeCalls)
				}
			} else if err != nil || third.Outcome != "succeeded" || third.Stage != "settled" || third.Attempt != 2 || repairCalls != 2 || removeCalls != 1 {
				t.Fatalf("third=%+v err=%v repairs=%d removals=%d", third, err, repairCalls, removeCalls)
			}
			if len(restoreIDs) != 2 || restoreIDs[0] == restoreIDs[1] {
				t.Fatalf("restore replayed an earlier failed request: %v", restoreIDs)
			}
		})
	}
}

func TestDaemonUpdateCeremonyContinuesAfterAdministratorStopsHost(t *testing.T) {
	t.Parallel()
	owner := serviceOwner{system: launchdservice.DefaultSystem(), observe: launchctlServiceObservation, drainVM: stopVMForUpdate}
	if os.Geteuid() == 0 {
		t.Skip("requires the service-user daemon branch")
	}
	value := testPaths(t.TempDir())
	value.owner = &owner
	value.service.Mode = serviceModeDaemon
	value.hostPlist = value.daemonPlist
	source := filepath.Join(filepath.Dir(value.app), ".Agent VMM Host.prepared.app")
	currentRelease := "release-old"
	jobLoaded := true
	drainCalls, replaceCalls, repairCalls, removeCalls := 0, 0, 0, 0
	owner.drainVM = func(context.Context, paths, string) error {
		drainCalls++
		return nil
	}
	owner.system.StateDiskOpenOwners = func(context.Context, string) ([]int, error) { return nil, nil }
	actions := updateApplicationActions{
		releaseID: func(path string) (string, error) {
			if path == value.app {
				return currentRelease, nil
			}
			if currentRelease == "release-old" {
				return "release-new", nil
			}
			return "release-old", nil
		},
		drain: func(paths) error {
			return drainWithContext(context.Background(), value, func(context.Context, ...string) error {
				if jobLoaded {
					return nil
				}
				return errHostServiceNotFound
			})
		},
		replace: func(string, string) error {
			replaceCalls++
			currentRelease = "release-new"
			return nil
		},
		repair: func(paths) error {
			repairCalls++
			if !jobLoaded {
				return errors.New("fixed Host job is absent")
			}
			return nil
		},
		startVM: func(context.Context, paths, string) error { return nil },
		remove:  func(string) error { removeCalls++; return nil },
	}
	run := func() (lifecycleOperation, error) {
		return runUpdateLifecycleOperation(value, "daemon-update", "release-new", func() error {
			return updateApplicationWith(value, source, "release-new", "daemon-update", actions)
		})
	}
	first, err := run()
	if err == nil || first.Stage != "stop" || drainCalls != 1 || replaceCalls != 0 {
		t.Fatalf("first=%+v err=%v drain=%d replace=%d", first, err, drainCalls, replaceCalls)
	}
	jobLoaded = false // fixed executor stopped the Host job
	second, err := run()
	if err == nil || second.Stage != "restore" || second.Attempt != 1 || drainCalls != 1 || replaceCalls != 1 || repairCalls != 1 {
		t.Fatalf("second=%+v err=%v drain=%d replace=%d repair=%d", second, err, drainCalls, replaceCalls, repairCalls)
	}
	// A failed executor installation does not consume the recovery-stage retry.
	third, err := run()
	if err == nil || third.Stage != "restore" || third.Attempt != 2 || repairCalls != 2 {
		t.Fatalf("third=%+v err=%v drain=%d replace=%d repair=%d remove=%d", third, err, drainCalls, replaceCalls, repairCalls, removeCalls)
	}
	jobLoaded = true // fixed executor installed the new Host job on retry
	fourth, err := run()
	if err != nil || fourth.Outcome != "succeeded" || fourth.Stage != "settled" || drainCalls != 1 || replaceCalls != 1 || repairCalls != 3 || removeCalls != 1 {
		t.Fatalf("fourth=%+v err=%v drain=%d replace=%d repair=%d remove=%d", fourth, err, drainCalls, replaceCalls, repairCalls, removeCalls)
	}
}

func TestUpdateSameRequestRejectsDifferentSelectedRelease(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	first, err := runUpdateLifecycleOperation(value, "update-stable", "release-a", func() error {
		return errors.New("preparation interrupted")
	})
	if err == nil || first.TargetReleaseID != "release-a" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	secondActionCalled := false
	second, err := runUpdateLifecycleOperation(value, "update-stable", "release-b", func() error {
		secondActionCalled = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "different target") || second.TargetReleaseID != "release-a" || secondActionCalled {
		t.Fatalf("second=%+v err=%v actionCalled=%t", second, err, secondActionCalled)
	}
}

func TestUpdateSameRequestRejectsDifferentSelectedArtifact(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	first, err := runUpdateLifecycleOperationForArtifact(value, "update-stable", "release-a", strings.Repeat("a", 64), 1024, func() error {
		return updatePreparationError{descriptor: updatePreparationFailureDescriptor{Version: 1, Stage: "download", Code: "update_download_unavailable", Message: "network unavailable", Retryable: true}}
	})
	if err == nil || first.TargetArtifactSHA256 != strings.Repeat("a", 64) || first.TargetArtifactSize != 1024 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	called := false
	second, err := runUpdateLifecycleOperationForArtifact(value, "update-stable", "release-a", strings.Repeat("b", 64), 1024, func() error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "different target") || called || second.TargetArtifactSHA256 != strings.Repeat("a", 64) {
		t.Fatalf("second=%+v err=%v called=%t", second, err, called)
	}
}

func TestUpdatePreparationFailureRetriesAreDurablyBounded(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	digest := strings.Repeat("c", 64)
	run := func() (lifecycleOperation, error) {
		return runUpdateLifecycleOperationForArtifact(value, "update-retry", "release-a", digest, 2048, func() error {
			return updatePreparationError{descriptor: updatePreparationFailureDescriptor{Version: 1, Stage: "download", Code: "update_download_unavailable", Message: "network unavailable", Retryable: true}}
		})
	}
	for attempt := 1; attempt <= boundedOperationMaxAttempts; attempt++ {
		operation, err := run()
		if err == nil || operation.Attempt != attempt || operation.TargetArtifactSHA256 != digest || operation.TargetArtifactSize != 2048 {
			t.Fatalf("attempt=%d operation=%+v err=%v", attempt, operation, err)
		}
		if attempt < boundedOperationMaxAttempts && !operation.Retryable {
			t.Fatalf("attempt %d became terminal early: %+v", attempt, operation)
		}
		if attempt == boundedOperationMaxAttempts && (operation.Retryable || operation.FailureCode != "update_retry_limit_reached") {
			t.Fatalf("final attempt was not terminal: %+v", operation)
		}
	}
	called := false
	operation, err := runUpdateLifecycleOperationForArtifact(value, "update-retry", "release-a", digest, 2048, func() error {
		called = true
		return nil
	})
	if err == nil || called || operation.Attempt != boundedOperationMaxAttempts || operation.Retryable {
		t.Fatalf("terminal replay operation=%+v err=%v called=%t", operation, err, called)
	}
}

func TestDecodeUpdatePreparationFailureRejectsUnboundedOrUnknownInput(t *testing.T) {
	t.Parallel()
	valid := `{"version":1,"stage":"download","code":"update_download_unavailable","message":"network unavailable","retryable":true}`
	decoded, err := decodeUpdatePreparationFailure(strings.NewReader(valid))
	if err != nil || decoded.Stage != "download" || !decoded.Retryable {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	for _, input := range []string{
		`{"version":1,"stage":"download","code":"update_download_unavailable","message":"network unavailable","retryable":true,"extra":true}`,
		valid + `{}`,
		`{"version":1,"stage":"stop","code":"update_failed","message":"invalid stage","retryable":true}`,
	} {
		if _, err := decodeUpdatePreparationFailure(strings.NewReader(input)); err == nil {
			t.Fatalf("invalid descriptor accepted: %s", input)
		}
	}
}

func TestLifecycleOperationIsDurableAndRequestIdempotent(t *testing.T) {
	t.Parallel()
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
	observed := readLifecycleOperation(value)
	if observed == nil || observed.OperationID != first.OperationID || observed.Outcome != "succeeded" {
		t.Fatalf("durable operation=%+v", observed)
	}
}

func TestLifecycleOperationFailureIsRecorded(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	_, err := runLifecycleOperation(value, "remove", "request-2", func() error { return os.ErrPermission })
	if err == nil {
		t.Fatal("expected operation failure")
	}
	observed := readLifecycleOperation(value)
	if observed == nil || observed.Outcome != "failed" || observed.Kind != "remove" {
		t.Fatalf("failed operation=%+v", observed)
	}
}

func TestManagedLifecycleOperationRetriesFailedStableRequest(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	recorded, err := os.ReadFile(filepath.Join(value.applicationData, maintenanceReceiptName(operationID)))
	if err != nil || strings.Contains(string(recorded), "secret-token") || !strings.Contains(string(recorded), `"outcome":"failed"`) {
		t.Fatalf("non-secret terminal operation record missing or leaked material: %s err=%v", recorded, err)
	}
}

func TestRetryableManagedLifecycleFailureRetainsPlan(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	if !errors.Is(operationErr, errMaintenanceOwnerBusy) || operation.SecretDisposition != secretDispositionRetain ||
		operation.OperationID != operationID || operation.FailureCode != "owner_busy" || !operation.Retryable || operation.Outcome != "unknown" {
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	generated := lifecycleRequestID("")
	if generated == "" || !strings.HasPrefix(generated, "req-") {
		t.Fatalf("generated request ID = %q", generated)
	}
	if got := lifecycleRequestID("operator-request"); got != "operator-request" {
		t.Fatalf("explicit request ID changed to %q", got)
	}
}

func TestRegistrationStateOperationRetriesTransientSameRevision(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	if observed := readLifecycleOperation(value); observed == nil || observed.Outcome != "unknown" {
		t.Fatalf("pending operation outcome=%+v", observed)
	}
}

func TestLifecycleOperationSerializesDuplicateRequests(t *testing.T) {
	t.Parallel()
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

func TestRepairRetryKeepsRegistrationTargetAndRerunsPendingStage(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	if err := persistOperation(value, lifecycleOperation{OperationID: "operation-1", RequestID: "request-1", Kind: "repair", TargetRegistrationID: "registration-a", Stage: "registration", Outcome: "pending", Attempt: 1}); err != nil {
		t.Fatal(err)
	}
	called := false
	operation, err := runRepairLifecycleOperation(value, "request-1", "registration-a", func() error {
		called = true
		return persistRepairStage(value, "request-1", "registration-a", "complete")
	})
	if err != nil || !called || operation.Outcome != "succeeded" || operation.TargetRegistrationID != "registration-a" || operation.Stage != "complete" {
		t.Fatalf("operation=%+v called=%t err=%v", operation, called, err)
	}
	called = false
	if _, err := runRepairLifecycleOperation(value, "request-1", "registration-b", func() error { called = true; return nil }); err == nil || called {
		t.Fatalf("target reuse err=%v called=%t", err, called)
	}
}

func TestRepairFailureKeepsDurableRegistrationStageAndTarget(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	operation, err := runRepairLifecycleOperation(value, "request-failed", "registration-a", func() error {
		if err := persistRepairStage(value, "request-failed", "registration-a", "registration"); err != nil {
			return err
		}
		return context.DeadlineExceeded
	})
	if err == nil || operation.Outcome != "failed" || operation.Stage != "registration" || operation.FailureStage != "registration" || operation.TargetRegistrationID != "registration-a" || !operation.Retryable {
		t.Fatalf("operation=%+v err=%v", operation, err)
	}
	persisted := readLifecycleOperation(value)
	if persisted == nil || persisted.Stage != "registration" || persisted.FailureStage != "registration" || persisted.TargetRegistrationID != "registration-a" {
		t.Fatalf("persisted=%+v", persisted)
	}
}

func TestParseLifecycleOperationArgsRequiresOneOriginalID(t *testing.T) {
	t.Parallel()
	if got, err := parseLifecycleOperationArgs([]string{"--operation-id", "operation-1"}); err != nil || got != "operation-1" {
		t.Fatalf("operation=%q err=%v", got, err)
	}
	for _, args := range [][]string{nil, {"--operation-id", "bad/id"}, {"--operation-id", "one", "--operation-id", "two"}} {
		if _, err := parseLifecycleOperationArgs(args); err == nil {
			t.Fatalf("accepted arguments %v", args)
		}
	}
}

func TestReadLifecycleOperationByIDDoesNotSubstituteLatestOperation(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	if err := persistOperation(value, lifecycleOperation{OperationID: "operation-current", RequestID: "request-current", Kind: "repair", Outcome: "running"}); err != nil {
		t.Fatal(err)
	}
	if _, err := readLifecycleOperationByID(value, "operation-old"); err == nil {
		t.Fatal("a different current operation satisfied the requested ID")
	}
	operation, err := readLifecycleOperationByID(value, "operation-current")
	if err != nil || operation.OperationID != "operation-current" {
		t.Fatalf("operation=%+v err=%v", operation, err)
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
		applicationData: filepath.Join(home, "Library", "Application Support", "AgentVMM"), logs: filepath.Join(home, "Library", "Logs", "AgentVMM"), serviceLogs: filepath.Join(home, "Library", "Logs", "AgentVMM"),
		serviceConfigFile: filepath.Join(home, "Library", "Application Support", "AgentVMM", "service.json"),
	}
}

func TestAtomicWriteReplacesDefinition(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	if operationConfirmed("install-managed", serviceStatus{HostInstalled: true, HostLoaded: true, HostHealthy: true}) {
		t.Fatal("managed install recovery was confirmed without exact registration observation")
	}
}

func TestExactRegistrationReadClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want string
	}{
		{nil, "present"}, {status.Error(codes.NotFound, "absent"), "absent"},
		{status.Error(codes.Unavailable, "database unreadable"), "unreadable"},
		{context.DeadlineExceeded, "unreadable"},
	} {
		got, _ := classifyRegistrationRead(test.err)
		if got != test.want {
			t.Fatalf("read %v = %s, want %s", test.err, got, test.want)
		}
	}
}

func TestDisposedRegistrationFailureStaysTerminalThroughLifecycleJournal(t *testing.T) {
	t.Parallel()
	failure, err := status.New(codes.NotFound, "original registration is absent").WithDetails(&errdetails.ErrorInfo{Domain: hostv1.ErrorDomain, Reason: "LOCAL_DISPOSED"})
	if err != nil {
		t.Fatal(err)
	}
	get := func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error) {
		return nil, failure.Err()
	}
	set := func(context.Context, *hostv1.SetRemoteRegistrationStateRequest, ...grpc.CallOption) (*hostv1.SetRemoteRegistrationStateResponse, error) {
		t.Fatal("disposed registration was mutated")
		return nil, nil
	}
	err = setRemoteRegistrationStateWith(context.Background(), "disposed", hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, "original-control", get, set)
	_, code, _, retryable := lifecycleFailure("registration-enabled", err)
	if code != "local_disposed" || retryable {
		t.Fatalf("failure=%s retryable=%v err=%v", code, retryable, err)
	}
}

func TestRevokeResumesPreviouslyFailedExactRequest(t *testing.T) {
	t.Parallel()
	value := testPaths(t.TempDir())
	previous := lifecycleOperation{OperationID: "remove", RequestID: "remove", Kind: "registration-revoke", TargetRegistrationID: "failed", Outcome: "failed", Attempt: 1, FailureCode: "operation_failed", FailureMessage: "registration revision conflict"}
	if err := persistOperation(value, previous); err != nil {
		t.Fatal(err)
	}
	calls := 0
	action := func() error { calls++; return nil }
	operation, err := runLifecycleOperationWithFailedRetry(value, "registration-revoke", "remove", "failed", "", "", 0, true, action)
	if err != nil || operation.Outcome != "succeeded" || operation.Attempt != 2 || calls != 1 {
		t.Fatalf("operation=%+v calls=%d err=%v", operation, calls, err)
	}
	_, err = runLifecycleOperationWithFailedRetry(value, "registration-revoke", "remove", "neighbor", "", "", 0, true, action)
	if err == nil || calls != 1 {
		t.Fatal("request moved to another registration")
	}
	_, err = runLifecycleOperationWithFailedRetry(value, "registration-revoke", "remove", "failed", "", "", 0, true, action)
	if err != nil || calls != 1 {
		t.Fatal("completed removal replayed")
	}
}

type revocationRaceClient struct {
	hostv1.OperatorServiceClient
	reads, begins, completes int
	alwaysConflict           bool
}

func (c *revocationRaceClient) GetRemoteRegistration(_ context.Context, r *hostv1.GetRemoteRegistrationRequest, _ ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error) {
	c.reads++
	if r.RegistrationId != "failed" {
		return nil, status.Error(codes.PermissionDenied, "wrong target")
	}
	return &hostv1.GetRemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: r.RegistrationId, Revision: uint64(c.reads), State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED}}, nil
}
func (c *revocationRaceClient) BeginRemoteRevocation(_ context.Context, r *hostv1.BeginRemoteRevocationRequest, _ ...grpc.CallOption) (*hostv1.BeginRemoteRevocationResponse, error) {
	c.begins++
	if r.RequestId != "remove" || r.ExpectedRevision != uint64(c.reads) {
		return nil, status.Error(codes.InvalidArgument, "wrong identity")
	}
	if c.begins == 1 || c.alwaysConflict {
		conflict, _ := status.New(codes.Aborted, "registration revision changed").WithDetails(&errdetails.ErrorInfo{Domain: hostv1.ErrorDomain, Reason: "REGISTRATION_REVISION_CONFLICT"})
		return nil, conflict.Err()
	}
	return &hostv1.BeginRemoteRevocationResponse{Registration: &hostv1.RemoteRegistration{Id: r.RegistrationId, Revision: r.ExpectedRevision + 1}}, nil
}
func (c *revocationRaceClient) CompleteRemoteRevocation(_ context.Context, r *hostv1.CompleteRemoteRevocationRequest, _ ...grpc.CallOption) (*hostv1.CompleteRemoteRevocationResponse, error) {
	c.completes++
	if r.RegistrationId != "failed" || r.RequestId != "remove" || r.ExpectedRevision != uint64(c.reads+1) {
		return nil, status.Error(codes.InvalidArgument, "wrong completion")
	}
	return &hostv1.CompleteRemoteRevocationResponse{}, nil
}
func TestRevokeRefreshesExactRevisionWithinBound(t *testing.T) {
	t.Parallel()
	client := &revocationRaceClient{}
	if err := completeRemoteRevocation(context.Background(), client, "failed", "remove"); err != nil || client.reads != 2 || client.completes != 1 {
		t.Fatalf("client=%+v err=%v", client, err)
	}
	client = &revocationRaceClient{alwaysConflict: true}
	if err := completeRemoteRevocation(context.Background(), client, "failed", "remove"); !registrationRevisionConflict(err) || client.reads != 3 || client.completes != 0 {
		t.Fatalf("client=%+v err=%v", client, err)
	}
}

func TestLifecycleOperationRejectsAncestorSymlinkWithoutChangingTarget(t *testing.T) {
	t.Parallel()
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
