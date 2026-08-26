package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestXMLEscape(t *testing.T) {
	if got := xmlEscape("/Users/a&b<c>"); got != "/Users/a&amp;b&lt;c&gt;" {
		t.Fatalf("xmlEscape() = %q", got)
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
	if len(commands) != 2 || !strings.Contains(commands[0], hostLabel) || !strings.HasPrefix(commands[1], "print") {
		t.Fatalf("drain order=%v", commands)
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
	if calls != 2 || second.Outcome != "succeeded" || second.OperationID != first.OperationID || second.RequestID != first.RequestID {
		t.Fatalf("retried managed operation first=%+v second=%+v calls=%d", first, second, calls)
	}
}

func TestManagedLifecycleOperationDoesNotRetryPermanentFailure(t *testing.T) {
	value := testPaths(t.TempDir())
	calls := 0
	first, err := runManagedLifecycleOperation(value, "install-managed", "operation-terminal", func() error {
		calls++
		return os.ErrPermission
	})
	if err == nil || first.Retryable || first.FailureCode != "install_failed" {
		t.Fatalf("terminal managed operation=%+v err=%v", first, err)
	}
	if first.SecretDisposition != secretDispositionRetain {
		t.Fatalf("generic non-retryable error gained secret authority: %+v", first)
	}
	second, err := runManagedLifecycleOperation(value, "install-managed", "operation-terminal", func() error {
		calls++
		return nil
	})
	if err == nil || calls != 1 || second.OperationID != first.OperationID {
		t.Fatalf("terminal operation was retried: first=%+v second=%+v calls=%d err=%v", first, second, calls, err)
	}
}

func TestTerminalManagedLifecycleFailureClearsSecretPlan(t *testing.T) {
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
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("terminal operation error was lost")
	}
	if _, err := os.Stat(planDirectory); !os.IsNotExist(err) {
		t.Fatalf("terminal failure retained secret plan: %v", err)
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

func TestNonRetryableStorageFailureRetainsPlan(t *testing.T) {
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
	if operationErr == nil || operation.Retryable || operation.SecretDisposition != secretDispositionRetain {
		t.Fatalf("storage failure gained terminal authority: operation=%+v err=%v", operation, operationErr)
	}
	if _, err := finalizeManagedInstallOperation(value, operationID, operation, operationErr); err == nil {
		t.Fatal("storage failure error was lost")
	}
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("storage failure removed durable plan: %v", err)
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
	release, err := acquireOperationLock(value.operationFile)
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
		OperationID: "local-operation", RequestID: operationID, Kind: "install-managed",
		Outcome: "succeeded", Attempt: 1, StartedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	if err := persistOperation(value.operationFile, previous); err != nil {
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

func TestCleanupFailurePublishesRetainDisposition(t *testing.T) {
	value := testPaths(t.TempDir())
	operationID := "operation-cleanup-failure"
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(value.applicationData, "install-operations"), []byte("blocks cleanup path"), 0o600); err != nil {
		t.Fatal(err)
	}
	operation := lifecycleOperation{
		OperationID: "local-operation", RequestID: operationID, Kind: "install-managed",
		Outcome: "failed", SecretDisposition: secretDispositionDestroyTerminal,
		Attempt: 1, StartedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	if err := persistOperation(value.operationFile, operation); err != nil {
		t.Fatal(err)
	}
	published, err := finalizeManagedInstallOperation(value, operationID, operation, os.ErrPermission)
	if err == nil || published.SecretDisposition != secretDispositionRetain {
		t.Fatalf("cleanup failure published terminal disposition: operation=%+v err=%v", published, err)
	}
	recorded := readOperation(value.operationFile)
	if recorded == nil || recorded.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("cleanup retry evidence was not retained: %+v", recorded)
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
		OperationID: "local-operation", RequestID: "operation-unknown", Kind: "install-managed",
		Outcome: "pending", Attempt: 1, StartedAt: time.Now().UnixMilli(), UpdatedAt: time.Now().UnixMilli(),
	}
	if err := persistOperation(value.operationFile, previous); err != nil {
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
	if err := persistOperation(value.operationFile, first); err != nil {
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

func TestApplianceFactsDoNotCrossContaminateReadiness(t *testing.T) {
	healthy := &appliancev1.ApplianceStatus{
		ApplianceState: appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING,
		Health:         &appliancev1.ApplianceHealth{Healthy: true},
	}
	controller := &appliancev1.ApplianceStatus{
		ChannelState:   appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		RegistrationId: "registration-b",
		Health:         &appliancev1.ApplianceHealth{ReadinessNonce: []byte("nonce")},
	}
	inventory := &appliancev1.ApplianceStatus{
		InventoryWatermark: 1,
		RegistrationId:     "registration-c",
		Health:             &appliancev1.ApplianceHealth{},
	}

	facts := mergeApplianceFacts(operatorFacts{}, []*appliancev1.ApplianceStatus{healthy, controller, inventory})
	if !facts.applianceHealthy || !facts.controllerActive || !facts.inventoryComplete {
		t.Fatalf("diagnostic facts=%+v", facts)
	}
	if operationConfirmed("install-managed", serviceStatus{HostInstalled: true, HostLoaded: true, HostHealthy: true}) {
		t.Fatal("managed install recovery was confirmed without exact registration observation")
	}
}

func TestApplianceFactsDoNotProjectSalixReadiness(t *testing.T) {
	appliance := &appliancev1.ApplianceStatus{
		ApplianceState:     appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING,
		ChannelState:       appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		InventoryWatermark: 1,
		RegistrationId:     "registration-a",
		Health:             &appliancev1.ApplianceHealth{Healthy: true, ReadinessNonce: []byte("nonce")},
	}

	facts := mergeApplianceFacts(operatorFacts{}, []*appliancev1.ApplianceStatus{appliance})
	if !facts.applianceHealthy || !facts.controllerActive || !facts.inventoryComplete {
		t.Fatalf("appliance diagnostics were not preserved: %+v", facts)
	}
}
