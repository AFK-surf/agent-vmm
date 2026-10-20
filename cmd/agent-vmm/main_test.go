//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestManagedInspectEmitsVersionedOwnerFacts(t *testing.T) {
	observed := time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC)
	var arguments []string
	runtime := managedRuntime{
		now: func() time.Time { return observed },
		runHelper: func(_ context.Context, path string, args []string) ([]byte, []byte, error) {
			if path != "/tmp/lifecycle" {
				t.Fatalf("helper path = %q", path)
			}
			arguments = append([]string(nil), args...)
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostInstalled":true,"hostObserved":true,"hostLoaded":true,"hostRunning":true,"hostReadable":true,"hostHealthy":true,"operation":{"operationId":"operation-1","outcome":"succeeded","updatedAt":1789372800000}}`), nil, nil
		},
		getRegistration: func(context.Context, string, string) (*hostv1.RemoteRegistration, error) {
			return &hostv1.RemoteRegistration{Id: "registration-1", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, ConnectionState: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"inspect", "--json", "--lifecycle-helper", "/tmp/lifecycle", "--registration-id", "registration-1", "--service-type", "daemon", "--service-user", "runner"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if strings.Join(arguments, " ") != "status --registration-id registration-1 --service-type daemon --service-user runner" {
		t.Fatalf("helper arguments = %q", arguments)
	}
	var envelope managedEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 1 || envelope.State != "healthy" || envelope.Freshness != "current" || envelope.Partial {
		t.Fatalf("envelope = %+v", envelope)
	}
	if envelope.Target["registration_id"] != "registration-1" || envelope.Target["service_uid"].(float64) != 501 {
		t.Fatalf("target = %#v", envelope.Target)
	}
	operation := envelope.OperationRef.(map[string]any)
	if operation["owner"] != "lifecycle" || operation["id"] != "operation-1" {
		t.Fatalf("operation ref = %#v", operation)
	}
}

func TestManagedDiagnoseKeepsPartialFactsWhenOperatorIsUnavailable(t *testing.T) {
	runtime := managedRuntime{
		now: func() time.Time { return time.Unix(100, 0) },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostInstalled":true,"hostLoaded":true,"hostRunning":true,"hostReadable":false,"hostHealthy":false,"hostObservationIssue":"Authorization: Bearer should-not-leak"}`), nil, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"diagnose", "--json", "--lifecycle-helper", "/tmp/lifecycle", "--operator-socket", "/tmp/operator.sock"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "should-not-leak") {
		t.Fatalf("diagnose leaked a secret: %s", stdout.String())
	}
	var envelope managedEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Partial || envelope.State != "host_unreadable" {
		t.Fatalf("envelope = %+v", envelope)
	}
}

func TestManagedDiagnoseProjectsLayeredResources(t *testing.T) {
	runtime := managedRuntime{
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostObserved":true,"hostRunning":true,"hostReadable":true,"hostHealthy":true,"snapshotObservedAt":"2026-09-14T08:00:00Z","hostResources":{"memory_bytes":100},"guestResources":{"memory_bytes":200}}`), nil, nil
		},
		now: func() time.Time { return time.Date(2026, 9, 14, 8, 0, 1, 0, time.UTC) },
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"diagnose", "--json", "--lifecycle-helper", "/tmp/lifecycle"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	var body map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	resources := body["facts"].(map[string]any)["resources"].(map[string]any)
	if resources["host"].(map[string]any)["memory_bytes"].(float64) != 100 || resources["guest"].(map[string]any)["memory_bytes"].(float64) != 200 {
		t.Fatalf("resources=%#v", resources)
	}
}

func TestManagedInspectUsesCurrentHostObservationOverHistoricalStartup(t *testing.T) {
	runtime := managedRuntime{
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostObserved":true,"hostRunning":true,"hostReadable":true,"hostHealthy":true,"snapshotObservedAt":"2026-09-14T08:00:00Z","startup":{"observedAt":"2026-09-14T07:00:00Z","failureConfirmed":true,"failureStage":"listener","failureMessage":"historical failure"}}`), nil, nil
		},
		now: func() time.Time { return time.Date(2026, 9, 14, 8, 0, 1, 0, time.UTC) },
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"inspect", "--json", "--lifecycle-helper", "/tmp/lifecycle"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	var envelope managedEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.State != "healthy" || envelope.Freshness != "current" || envelope.Stage != "healthy" || envelope.Issue != nil {
		t.Fatalf("current Host observation = %+v", envelope)
	}
}

func TestManagedOperationFollowReusesOriginalHostOperation(t *testing.T) {
	states := []hostv1.OperationState{hostv1.OperationState_OPERATION_STATE_RUNNING, hostv1.OperationState_OPERATION_STATE_SUCCEEDED}
	calls, helperCalls := 0, 0
	var stdout, stderr bytes.Buffer
	runtime := managedRuntime{
		now: func() time.Time { return time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC) },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			helperCalls++
			if helperCalls != 1 {
				t.Fatal("Host follow repeated the lifecycle snapshot")
			}
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		getOperation: func(_ context.Context, socket, id string) (*hostv1.Operation, error) {
			if socket != "/tmp/operator.sock" || id != "operation-7" {
				t.Fatalf("operation target = %q %q", socket, id)
			}
			state := states[calls]
			calls++
			return &hostv1.Operation{Id: id, Kind: "vm_start", State: state, Stage: "start", UpdatedAt: timestamppb.New(time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC))}, nil
		},
		sleep: func(_ context.Context, duration time.Duration) error {
			if stdout.Len() == 0 {
				t.Fatal("follow did not stream the first observation before polling")
			}
			if duration != 2*time.Second {
				t.Fatalf("poll interval=%s", duration)
			}
			return nil
		},
	}
	exit := runManagedCLI([]string{"operation", "follow", "--owner", "host", "--operation-id", "operation-7", "--operator-socket", "/tmp/operator.sock"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 || calls != 2 {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", exit, calls, stdout.String(), stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"state":"running"`) || !strings.Contains(lines[1], `"state":"succeeded"`) {
		t.Fatalf("follow output = %q", lines)
	}
	for _, line := range lines {
		if !strings.Contains(line, `"id":"operation-7"`) {
			t.Fatalf("follow changed operation reference: %s", line)
		}
	}
}

func TestManagedInspectReturnsPartialEnvelopeAtItsDeadline(t *testing.T) {
	runtime := managedRuntime{
		now: func() time.Time { return time.Unix(100, 0) },
		runHelper: func(ctx context.Context, _ string, _ []string) ([]byte, []byte, error) {
			<-ctx.Done()
			return nil, nil, ctx.Err()
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	envelope, err := runtime.inspect(ctx, "inspect", managedOptions{serviceUser: "runner"})
	if err != nil || !envelope.Partial || envelope.State != "unknown" {
		t.Fatalf("envelope=%+v err=%v", envelope, err)
	}
	issue, ok := envelope.Issue.(managedIssue)
	if !ok || issue.Code != "observation_timeout" || issue.Retryable == nil || !*issue.Retryable {
		t.Fatalf("issue=%#v", envelope.Issue)
	}
}

func TestManagedFailedOperationsProjectStandardIssues(t *testing.T) {
	status := []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`)
	t.Run("lifecycle", func(t *testing.T) {
		runtime := managedRuntime{runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] == "status" {
				return status, nil, nil
			}
			return []byte(`{"operationId":"operation-1","kind":"repair","targetRegistrationId":"registration-a","stage":"registration","outcome":"failed","failureStage":"registration","failureCode":"launch_failed","failureMessage":"token=do-not-leak","retryable":true,"updatedAt":1000}`), nil, nil
		}}
		var stdout, stderr bytes.Buffer
		exit := runManagedCLI([]string{"operation", "get", "--owner", "lifecycle", "--operation-id", "operation-1", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
		if exit != 0 || stderr.Len() != 0 || strings.Contains(stdout.String(), "do-not-leak") {
			t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
		}
		var body map[string]any
		if json.Unmarshal(stdout.Bytes(), &body) != nil {
			t.Fatal("operation output is not JSON")
		}
		issue := body["issue"].(map[string]any)
		if issue["code"] != "launch_failed" || issue["layer"] != "lifecycle" || issue["retryable"] != true {
			t.Fatalf("issue=%#v", issue)
		}
		if body["stage"] != "registration" || body["target"].(map[string]any)["registration_id"] != "registration-a" {
			t.Fatalf("stage/target=%#v %#v", body["stage"], body["target"])
		}
	})
	t.Run("host unknown retryability", func(t *testing.T) {
		runtime := managedRuntime{
			runHelper: func(context.Context, string, []string) ([]byte, []byte, error) { return status, nil, nil },
			getOperation: func(context.Context, string, string) (*hostv1.Operation, error) {
				return &hostv1.Operation{Id: "operation-2", State: hostv1.OperationState_OPERATION_STATE_FAILED, Error: &hostv1.OperationError{Reason: "vm_failed", Message: "failed"}}, nil
			},
		}
		var stdout, stderr bytes.Buffer
		exit := runManagedCLI([]string{"operation", "get", "--owner", "host", "--operation-id", "operation-2", "--operator-socket", "/tmp/operator.sock", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
		var body map[string]any
		if exit != 0 || json.Unmarshal(stdout.Bytes(), &body) != nil || body["issue"].(map[string]any)["retryable"] != nil {
			t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
		}
	})
}

func TestManagedBoundsAndRedaction(t *testing.T) {
	if _, err := parseManagedOptions("logs", "", []string{"--component", "host", "--limit", "101"}); err == nil {
		t.Fatal("log page above 100 was accepted")
	}
	input := "Authorization: Basic dXNlcjpwYXNz\nX-API-Key: sk-test\nCookie: session=hidden\ntoken=def \"credential\":\"ghi\" AWS_ACCESS_KEY_ID=AKIATEST\n{\"authorization\":\"Basic json-secret\",\"cookie\":\"session=json-cookie\"}"
	redacted := redactManagedText(input)
	for _, secret := range []string{"dXNlcjpwYXNz", "sk-test", "session=hidden", "def", "ghi", "AKIATEST", "json-secret", "json-cookie"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redaction leaked %q: %s", secret, redacted)
		}
	}
	redactedMap := redactManagedMap(map[string]any{"api_key": "plain-secret", "nested": map[string]any{"authorization": "Basic hidden", "task_body": "private task"}})
	if redactedMap["api_key"] != "[REDACTED]" || redactedMap["nested"].(map[string]any)["authorization"] != "[REDACTED]" || redactedMap["nested"].(map[string]any)["task_body"] != "[REDACTED]" {
		t.Fatalf("structured redaction = %#v", redactedMap)
	}
	path := filepath.Join(t.TempDir(), "log")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), managedLogBytes+128), 0o600); err != nil {
		t.Fatal(err)
	}
	records, _, gap, err := readManagedLocalLogFiles([]string{path}, managedOptions{})
	if err != nil || len(records) != 1 || len(records[0].Data) != managedLogBytes || !gap {
		t.Fatalf("bounded logs=%d gap=%t err=%v", len(records), gap, err)
	}
}

func TestManagedLogJSONHonorsEncodedOneMiBBudget(t *testing.T) {
	for _, test := range []struct {
		name    string
		records []managedLogRecord
	}{
		{name: "escaping", records: []managedLogRecord{{Cursor: "cursor-last", Data: strings.Repeat("\"\\\n", managedLogBytes/2)}}},
		{name: "record metadata", records: func() []managedLogRecord {
			records := make([]managedLogRecord, 100)
			for index := range records {
				records[index] = managedLogRecord{Cursor: strings.Repeat("c", 4096), Data: strings.Repeat("x", 20_000)}
			}
			return records
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := managedRuntime{
				runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
					return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
				},
				getEnvironment: func(context.Context, string, string) (*hostv1.AgentEnvironment, error) {
					return &hostv1.AgentEnvironment{Id: "environment-1", Containers: []*hostv1.Container{{Id: "container-1"}}}, nil
				},
				readLogs: func(context.Context, string, string, string, string, int, bool) ([]managedLogRecord, string, bool, error) {
					return test.records, "cursor-last", true, nil
				},
			}
			var stdout, stderr bytes.Buffer
			exit := runManagedCLI([]string{"logs", "--json", "--include-container-output", "--lifecycle-helper", "/tmp/lifecycle", "--operator-socket", "/tmp/operator.sock", "--environment-id", "environment-1", "--container-id", "container-1", "--limit", "100"}, &stdout, &stderr, runtime)
			var envelope managedEnvelope
			if exit != 0 || stderr.Len() != 0 || stdout.Len() > managedLogBytes || json.Unmarshal(stdout.Bytes(), &envelope) != nil {
				t.Fatalf("exit=%d bytes=%d stderr=%s", exit, stdout.Len(), stderr.String())
			}
			if envelope.Cursor != "cursor-last" || !envelope.Gap || len(envelope.Logs) == 0 {
				t.Fatalf("envelope=%+v", envelope)
			}
			truncated := false
			for _, record := range envelope.Logs {
				truncated = truncated || record.Truncated
			}
			if !truncated {
				t.Fatal("oversize encoded log output was not marked truncated")
			}
		})
	}
}

func TestManagedInspectRejectsMissingTargetIdentity(t *testing.T) {
	runtime := managedRuntime{
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"hostInstalled":false}`), nil, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"inspect", "--json", "--lifecycle-helper", "/tmp/lifecycle"}, &stdout, &stderr, runtime)
	if exit != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "did not identify one service UID") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
}

func TestManagedInspectResolvesBFTWorkloadThroughExistingOwners(t *testing.T) {
	runtime := managedRuntime{
		now: func() time.Time { return time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC) },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostObserved":true,"hostRunning":true,"hostReadable":true,"hostHealthy":true,"operatorSocket":"/tmp/operator.sock"}`), nil, nil
		},
		getEnvironment: func(_ context.Context, socket, id string) (*hostv1.AgentEnvironment, error) {
			if socket != "/tmp/operator.sock" || id != "environment-1" {
				t.Fatalf("environment target=%q %q", socket, id)
			}
			return &hostv1.AgentEnvironment{Id: id, Origin: hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE, InventoryState: hostv1.InventoryState_INVENTORY_STATE_AVAILABLE, Owner: &hostv1.EnvironmentOwner{RegistrationId: "registration-1"}}, nil
		},
		getRegistration: func(_ context.Context, socket, id string) (*hostv1.RemoteRegistration, error) {
			return &hostv1.RemoteRegistration{Id: id, Allocations: []*hostv1.RemoteAllocation{{
				AllocationId: "allocation-1", EnvironmentId: "environment-1", Generation: 7,
			}}}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"inspect", "--json", "--lifecycle-helper", "/tmp/lifecycle", "--registration-id", "registration-1", "--environment-id", "environment-1", "--workload-id", "allocation-1"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	var envelope managedEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Target["scope_status"] != "resolved" || envelope.Target["allocation_id"] != "allocation-1" || envelope.Target["allocation_generation"].(float64) != 7 {
		t.Fatalf("resolved target=%#v", envelope.Target)
	}
}

func TestManagedScopeValidationRejectsAmbiguousOrCrossOwnedTargets(t *testing.T) {
	status := map[string]any{"hostReadable": true, "operatorSocket": "/tmp/operator.sock"}
	base := managedOptions{registrationID: "registration-1", workloadID: "allocation-1"}
	tests := []struct {
		name         string
		opts         managedOptions
		environment  *hostv1.AgentEnvironment
		registration *hostv1.RemoteRegistration
	}{
		{name: "missing workload", opts: base, registration: &hostv1.RemoteRegistration{Id: "registration-1"}},
		{name: "multiple workload allocations", opts: base, registration: &hostv1.RemoteRegistration{Id: "registration-1", Allocations: []*hostv1.RemoteAllocation{
			{AllocationId: "allocation-1"},
			{AllocationId: "allocation-1"},
		}}},
		{name: "allocation bound", opts: base, registration: func() *hostv1.RemoteRegistration {
			allocations := make([]*hostv1.RemoteAllocation, 101)
			return &hostv1.RemoteRegistration{Id: "registration-1", Allocations: allocations}
		}()},
		{name: "cross registration environment", opts: managedOptions{registrationID: "registration-1", environmentID: "environment-1"}, environment: &hostv1.AgentEnvironment{Id: "environment-1", Owner: &hostv1.EnvironmentOwner{RegistrationId: "registration-2"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := managedRuntime{
				getEnvironment: func(context.Context, string, string) (*hostv1.AgentEnvironment, error) {
					if test.environment == nil {
						return nil, errors.New("not found")
					}
					return test.environment, nil
				},
				getRegistration: func(context.Context, string, string) (*hostv1.RemoteRegistration, error) {
					return test.registration, nil
				},
			}
			if _, err := runtime.resolveScope(context.Background(), status, test.opts); err == nil {
				t.Fatal("invalid scope resolved")
			}
		})
	}
}

func TestManagedCommandsRejectUnverifiableScopeCombinations(t *testing.T) {
	tests := []struct {
		command    string
		subcommand string
		arguments  []string
	}{
		{command: "logs", arguments: []string{"--component", "host", "--registration-id", "registration-1"}},
		{command: "logs", arguments: []string{"--component", "guest", "--environment-id", "environment-1"}},
		{command: "operation", subcommand: "get", arguments: []string{"--owner", "host", "--operation-id", "operation-1", "--environment-id", "environment-1"}},
		{command: "operation", subcommand: "follow", arguments: []string{"--owner", "lifecycle", "--operation-id", "operation-1", "--registration-id", "registration-1"}},
	}
	for _, test := range tests {
		if _, err := parseManagedOptions(test.command, test.subcommand, test.arguments); err == nil {
			t.Fatalf("accepted %s %s %v", test.command, test.subcommand, test.arguments)
		}
	}
}

func TestManagedContainerLogsHideTaskOutputByDefault(t *testing.T) {
	runtime := managedRuntime{
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		getEnvironment: func(context.Context, string, string) (*hostv1.AgentEnvironment, error) {
			return &hostv1.AgentEnvironment{Id: "environment-1", Owner: &hostv1.EnvironmentOwner{RegistrationId: "registration-1"}, Containers: []*hostv1.Container{{Id: "container-1"}}}, nil
		},
		readLogs: func(context.Context, string, string, string, string, int, bool) ([]managedLogRecord, string, bool, error) {
			return []managedLogRecord{{Cursor: "cursor-1", Data: "private task body"}}, "cursor-1", false, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"logs", "--json", "--lifecycle-helper", "/tmp/lifecycle", "--operator-socket", "/tmp/operator.sock", "--registration-id", "registration-1", "--environment-id", "environment-1", "--container-id", "container-1"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 || strings.Contains(stdout.String(), "private task body") || !strings.Contains(stdout.String(), "include-container-output") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
}

func TestManagedVMStopAcknowledgesWholeScopeAndRechecksFence(t *testing.T) {
	vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "boot-7"}
	summary := &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true, KnownWorkloads: []*hostv1.ResourceReference{{Kind: "registration", ResourceId: "registration-1", Generation: 4}, {Kind: "environment", EnvironmentId: "env-local"}}}
	calls := 0
	runtime := managedRuntime{
		getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) { return summary, nil },
		getVM:            func(context.Context, string) (*hostv1.VM, error) { calls++; return vm, nil },
		stopVM: func(_ context.Context, _ string, requestID string, force bool, fence *hostv1.VMMutationFence) (*hostv1.Operation, error) {
			if requestID != "request-7" || !force {
				t.Fatalf("request=%q force=%t", requestID, force)
			}
			if !fence.GetWholeVmImpactAcknowledged() || len(fence.GetObservedWorkloads()) != 2 {
				t.Fatalf("fence=%+v", fence)
			}
			return &hostv1.Operation{Id: "operation-7", RequestId: requestID, State: hostv1.OperationState_OPERATION_STATE_SUCCEEDED, Stage: "stopped"}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"vm", "stop", "--request-id", "request-7", "--force", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || calls != 1 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"impact_scope":"whole_vm"`) || !strings.Contains(stdout.String(), `"known_workload_count":2`) || !strings.Contains(stdout.String(), `"id":"operation-7"`) {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", exit, calls, stdout.String(), stderr.String())
	}
}

func TestManagedVMMutationRejectsChangedBootFence(t *testing.T) {
	called := false
	runtime := managedRuntime{
		now: time.Now,
		getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
			return &hostv1.GetVMImpactSummaryResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "before"}, Complete: true}, nil
		},
		getVM: func(context.Context, string) (*hostv1.VM, error) {
			return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "after"}, nil
		},
		stopVM: func(context.Context, string, string, bool, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
			called = true
			return nil, nil
		},
	}
	var stdout, stderr bytes.Buffer
	if exit := runManagedCLI([]string{"vm", "stop", "--request-id", "request-1", "--json"}, &stdout, &stderr, runtime); exit != 1 || called || !strings.Contains(stderr.String(), "VM changed after observation") {
		t.Fatalf("exit=%d called=%t stderr=%s", exit, called, stderr.String())
	}
}

func TestManagedVMMutationTimeoutKeepsSameRequestIdentity(t *testing.T) {
	vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
	runtime := managedRuntime{now: time.Now, getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
		return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true}, nil
	}, getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil }, startVM: func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
		return nil, context.DeadlineExceeded
	}}
	// Model the public deadline after owner acceptance.
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	envelope, err := runtime.vm(deadline, "start", managedOptions{requestID: "request-stable"})
	if err != nil || envelope.State != "unknown_outcome" || envelope.Facts["request_id"] != "request-stable" || envelope.NextAction[0].Action != "retry_same_request_id" {
		t.Fatalf("envelope=%+v err=%v", envelope, err)
	}
}

func TestManagedVMMutationTreatsUntypedTransportFailuresAsUnknown(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "owner response deadline exceeded")},
		{name: "unavailable", err: status.Error(codes.Unavailable, "connection lost")},
		{name: "canceled", err: status.Error(codes.Canceled, "stream canceled")},
		{name: "reset", err: errors.New("connection reset by peer")},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
			runtime := managedRuntime{
				now: time.Now,
				getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
					return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true}, nil
				},
				getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil },
				startVM: func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
					return nil, test.err
				},
			}
			envelope, err := runtime.vm(context.Background(), "start", managedOptions{requestID: "request-stable"})
			if err != nil || envelope.State != "unknown_outcome" || envelope.Facts["request_id"] != "request-stable" || envelope.NextAction[0].Action != "retry_same_request_id" {
				t.Fatalf("envelope=%+v err=%v", envelope, err)
			}
		})
	}
}

func TestManagedVMMutationTypedHostRejectionIsDefinite(t *testing.T) {
	value, detailErr := status.New(codes.Aborted, "VM fence changed").WithDetails(&errdetails.ErrorInfo{Reason: "VM_FENCE_CONFLICT"})
	if detailErr != nil {
		t.Fatal(detailErr)
	}
	vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
	runtime := managedRuntime{
		now: time.Now,
		getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
			return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true}, nil
		},
		getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil },
		startVM: func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
			return nil, value.Err()
		},
	}
	if _, err := runtime.vm(context.Background(), "start", managedOptions{requestID: "request-stable"}); err == nil || !strings.Contains(err.Error(), "Host owner rejected") {
		t.Fatalf("typed rejection err=%v", err)
	}
}

func TestManagedVMMutationTerminalPersistenceFailureCarriesOperationRepair(t *testing.T) {
	value, detailErr := status.New(codes.Unavailable, "terminal result was not saved").WithDetails(&errdetails.ErrorInfo{
		Reason:   "VM_TERMINAL_PERSIST_FAILED",
		Metadata: map[string]string{"operation_id": "operation-1", "request_id": "request-stable"},
	})
	if detailErr != nil {
		t.Fatal(detailErr)
	}
	vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
	runtime := managedRuntime{
		now: time.Now,
		getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
			return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true}, nil
		},
		getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil },
		startVM: func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
			return nil, value.Err()
		},
	}
	envelope, err := runtime.vm(context.Background(), "start", managedOptions{requestID: "request-stable"})
	operationRef, _ := envelope.OperationRef.(map[string]any)
	if err != nil || envelope.Stage != "terminal_result_not_saved" || operationRef["id"] != "operation-1" || len(envelope.NextAction) != 2 {
		t.Fatalf("envelope=%+v err=%v", envelope, err)
	}
}

func TestManagedVMMutationReplayActionsMatchStoredState(t *testing.T) {
	for _, test := range []struct {
		name       string
		state      hostv1.OperationState
		stage      string
		nextAction string
		partial    bool
	}{
		{name: "accepted", state: hostv1.OperationState_OPERATION_STATE_RUNNING, stage: "started", nextAction: "operation_follow"},
		{name: "interrupted", state: hostv1.OperationState_OPERATION_STATE_INTERRUPTED, stage: "interrupted", nextAction: "vm_status", partial: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
			runtime := managedRuntime{
				now: time.Now,
				getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
					return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Complete: true}, nil
				},
				getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil },
				startVM: func(context.Context, string, string, *hostv1.VMMutationFence) (*hostv1.Operation, error) {
					return &hostv1.Operation{Id: "operation-1", RequestId: "request-stable", State: test.state, Stage: test.stage}, nil
				},
			}
			envelope, err := runtime.vm(context.Background(), "start", managedOptions{requestID: "request-stable"})
			if err != nil || envelope.Partial != test.partial || len(envelope.NextAction) != 1 || envelope.NextAction[0].Action != test.nextAction {
				t.Fatalf("envelope=%+v err=%v", envelope, err)
			}
		})
	}
}

func TestManagedPartialImpactSummaryDoesNotNarrowWholeVMAction(t *testing.T) {
	vm := &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}
	called := 0
	runtime := managedRuntime{
		now: time.Now,
		getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
			return &hostv1.GetVMImpactSummaryResponse{Vm: vm, Issue: "known workload summary exceeds the 100 item bound", Overflow: true}, nil
		},
		getVM: func(context.Context, string) (*hostv1.VM, error) { return vm, nil },
		startVM: func(_ context.Context, _ string, requestID string, fence *hostv1.VMMutationFence) (*hostv1.Operation, error) {
			called++
			if !fence.GetWholeVmImpactAcknowledged() {
				t.Fatal("whole-VM impact was not acknowledged")
			}
			return &hostv1.Operation{Id: "operation", RequestId: requestID, State: hostv1.OperationState_OPERATION_STATE_SUCCEEDED}, nil
		},
	}
	start, err := runtime.vm(context.Background(), "start", managedOptions{requestID: "start"})
	startIssue, issueOK := start.Issue.(managedIssue)
	if err != nil || called != 1 || !start.Partial || !issueOK || startIssue.Code != "impact_summary_partial" || start.Facts["impact_scope"] != "whole_vm" {
		t.Fatalf("stopped start called=%d envelope=%+v err=%v", called, start, err)
	}
}

func TestManagedVMStatusReportsPartialBoundedSummary(t *testing.T) {
	runtime := managedRuntime{now: time.Now, getImpactSummary: func(context.Context, string) (*hostv1.GetVMImpactSummaryResponse, error) {
		return &hostv1.GetVMImpactSummaryResponse{
			Vm:    &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED},
			Issue: "known workload summary is unavailable",
		}, nil
	}}
	envelope, err := runtime.vm(context.Background(), "status", managedOptions{})
	issue, issueOK := envelope.Issue.(managedIssue)
	if err != nil || envelope.State != "stopped" || !envelope.Partial || !issueOK || issue.Code != "impact_summary_partial" {
		t.Fatalf("envelope=%+v err=%v", envelope, err)
	}
}

func TestManagedRepairReturnsAdministratorActionForExitedSystemJob(t *testing.T) {
	calls := 0
	runtime := managedRuntime{runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
		calls++
		if args[0] != "status" {
			t.Fatalf("unexpected mutation: %v", args)
		}
		return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"system/com.afk-surf.agent-vmm-host","hostInstalled":true,"hostObserved":true,"hostLoaded":true,"hostRunning":false}`), nil, nil
	}}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"repair", "--request-id", "request-1", "--json", "--lifecycle-helper", "/tmp/lifecycle"}, &stdout, &stderr, runtime)
	if exit != 0 || calls != 1 || !strings.Contains(stdout.String(), `"state":"needs_administrator"`) || !strings.Contains(stdout.String(), `"action":"service_start"`) {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", exit, calls, stdout.String(), stderr.String())
	}
}

func TestManagedRepairTransportLossReturnsSameTargetUnknownOutcome(t *testing.T) {
	calls := 0
	runtime := managedRuntime{now: time.Now, runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
		calls++
		if args[0] == "status" {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostRunning":true}`), nil, nil
		}
		return nil, []byte("connection lost"), context.DeadlineExceeded
	}}
	envelope, err := runtime.repair(context.Background(), managedOptions{requestID: "request-1", registrationID: "registration-1", lifecycle: "/tmp/lifecycle"})
	if err != nil || calls != 2 || envelope.State != "unknown_outcome" || envelope.Facts["request_id"] != "request-1" || envelope.NextAction[0].Action != "retry_same_request_id" {
		t.Fatalf("envelope=%+v calls=%d err=%v", envelope, calls, err)
	}
}

func TestManagedCleanupExecuteRechecksExactPlanAndVolumes(t *testing.T) {
	planCalls, executeCalls := 0, 0
	plan := &hostv1.CleanupPlan{EnvironmentId: "environment-1", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, Revision: 9, Impacts: []*hostv1.CleanupImpact{
		{Resource: &hostv1.ResourceReference{Kind: "container", EnvironmentId: "environment-1", ResourceId: "container-1", GenerationId: "generation-1"}, Effect: "writable snapshot and logs will be deleted", Irreversible: true},
		{Resource: &hostv1.ResourceReference{Kind: "volume", EnvironmentId: "environment-1", ResourceId: "volume-1"}, Effect: "named volume and all contents will be deleted", Irreversible: true},
	}}
	runtime := managedRuntime{
		now: time.Now,
		planCleanup: func(_ context.Context, socket, environmentID string, kind hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
			planCalls++
			if socket == "" || environmentID != "environment-1" || kind != hostv1.CleanupKind_CLEANUP_KIND_DISCARD {
				t.Fatalf("plan target=%q %q %v", socket, environmentID, kind)
			}
			return plan, nil
		},
		executeCleanup: func(_ context.Context, _ string, requestID string, current *hostv1.CleanupPlan, volumes []string) (*hostv1.Operation, error) {
			executeCalls++
			if requestID != "cleanup-1" || current.GetRevision() != 9 || len(volumes) != 1 || volumes[0] != "volume-1" {
				t.Fatalf("execute request=%q plan=%+v volumes=%v", requestID, current, volumes)
			}
			return &hostv1.Operation{Id: "operation-1", RequestId: requestID, State: hostv1.OperationState_OPERATION_STATE_SUCCEEDED, Stage: "complete"}, nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"cleanup", "execute", "--environment-id", "environment-1", "--kind", "discard", "--plan-revision", "9", "--request-id", "cleanup-1", "--confirm-irreversible", "--confirm-volume", "volume-1", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || planCalls != 0 || executeCalls != 1 || stderr.Len() != 0 {
		t.Fatalf("exit=%d plan=%d execute=%d stdout=%s stderr=%s", exit, planCalls, executeCalls, stdout.String(), stderr.String())
	}
	var body map[string]any
	if json.Unmarshal(stdout.Bytes(), &body) != nil || body["state"] != "succeeded" {
		t.Fatalf("body=%s", stdout.String())
	}
	facts := body["facts"].(map[string]any)
	confirmed := facts["confirmed_volume_ids"].([]any)
	if facts["reclaimable_bytes"] != nil || facts["reclaimable_bytes_available"] != false || len(confirmed) != 1 || confirmed[0] != "volume-1" {
		t.Fatalf("facts=%#v", facts)
	}
}

func TestManagedCleanupTypedStaleResultRequiresNewPlan(t *testing.T) {
	runtime := managedRuntime{
		now: time.Now,
		planCleanup: func(context.Context, string, string, hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
			t.Fatal("cleanup execute pre-read the current plan")
			return nil, nil
		},
		executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
			value, detailErr := status.New(codes.Aborted, "cleanup plan is stale").WithDetails(&errdetails.ErrorInfo{Reason: "CLEANUP_PLAN_STALE"})
			if detailErr != nil {
				t.Fatal(detailErr)
			}
			return nil, value.Err()
		},
	}
	_, err := runtime.cleanup(context.Background(), "execute", managedOptions{
		environmentID: "environment-1",
		cleanupKind:   "reset",
		planRevision:  9,
		requestID:     "cleanup-1",
	})
	if err == nil || !strings.Contains(err.Error(), "request a new plan") {
		t.Fatalf("typed stale cleanup err=%v", err)
	}
}

func TestManagedCleanupAdmissionTimeoutIsDefiniteBeforeOperationAcceptance(t *testing.T) {
	runtime := managedRuntime{
		now: time.Now,
		planCleanup: func(context.Context, string, string, hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
			return &hostv1.CleanupPlan{EnvironmentId: "environment-1", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 9}, nil
		},
		executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
			value, detailErr := status.New(codes.DeadlineExceeded, "cleanup admission deadline exceeded").WithDetails(&errdetails.ErrorInfo{Reason: "CLEANUP_ADMISSION_TIMEOUT"})
			if detailErr != nil {
				t.Fatal(detailErr)
			}
			return nil, value.Err()
		},
	}
	_, err := runtime.cleanup(context.Background(), "execute", managedOptions{
		environmentID: "environment-1",
		cleanupKind:   "reset",
		planRevision:  9,
		requestID:     "cleanup-1",
	})
	if err == nil || !strings.Contains(err.Error(), "before the Host accepted an operation") || !strings.Contains(err.Error(), "request a new plan") {
		t.Fatalf("cleanup admission timeout err=%v", err)
	}
}

func TestManagedCleanupTreatsUntypedTransportFailuresAsUnknown(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "owner response deadline exceeded")},
		{name: "unavailable", err: status.Error(codes.Unavailable, "connection lost")},
		{name: "canceled", err: status.Error(codes.Canceled, "stream canceled")},
		{name: "reset", err: errors.New("connection reset by peer")},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := managedRuntime{
				now: time.Now,
				executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
					return nil, test.err
				},
			}
			envelope, err := runtime.cleanup(context.Background(), "execute", managedOptions{
				environmentID: "environment-1",
				cleanupKind:   "reset",
				planRevision:  9,
				requestID:     "cleanup-1",
			})
			if err != nil || envelope.State != "unknown_outcome" || envelope.Facts["request_id"] != "cleanup-1" || envelope.NextAction[0].Action != "retry_same_request_id" {
				t.Fatalf("generic cleanup transport envelope=%+v err=%v", envelope, err)
			}
		})
	}
}

func TestManagedCleanupTerminalPersistenceFailureCarriesOperationRepair(t *testing.T) {
	runtime := managedRuntime{
		now: time.Now,
		executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
			value, detailErr := status.New(codes.Unavailable, "terminal result was not saved").WithDetails(&errdetails.ErrorInfo{
				Reason: "CLEANUP_TERMINAL_PERSIST_FAILED",
				Metadata: map[string]string{
					"operation_id": "operation-1",
					"request_id":   "cleanup-1",
				},
			})
			if detailErr != nil {
				t.Fatal(detailErr)
			}
			return nil, value.Err()
		},
	}
	envelope, err := runtime.cleanup(context.Background(), "execute", managedOptions{
		environmentID: "environment-1",
		cleanupKind:   "reset",
		planRevision:  9,
		requestID:     "cleanup-1",
	})
	operationRef, _ := envelope.OperationRef.(map[string]any)
	if err != nil || envelope.State != "unknown_outcome" || envelope.Stage != "terminal_result_not_saved" || operationRef["id"] != "operation-1" || len(envelope.NextAction) != 2 || envelope.NextAction[0].Action != "retry_same_request_id" || envelope.NextAction[1].Action != "service_restart" {
		t.Fatalf("terminal persistence envelope=%+v err=%v", envelope, err)
	}
}

func TestManagedCleanupAdmissionReconciliationFailureIsUnknown(t *testing.T) {
	runtime := managedRuntime{
		now: time.Now,
		executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
			value, detailErr := status.New(codes.Unavailable, "request result could not be read").WithDetails(&errdetails.ErrorInfo{
				Reason:   "CLEANUP_ADMISSION_OUTCOME_UNKNOWN",
				Metadata: map[string]string{"request_id": "cleanup-1"},
			})
			if detailErr != nil {
				t.Fatal(detailErr)
			}
			return nil, value.Err()
		},
	}
	envelope, err := runtime.cleanup(context.Background(), "execute", managedOptions{
		environmentID: "environment-1",
		cleanupKind:   "reset",
		planRevision:  9,
		requestID:     "cleanup-1",
	})
	if err != nil || envelope.State != "unknown_outcome" || envelope.Facts["request_id"] != "cleanup-1" || len(envelope.NextAction) != 1 || envelope.NextAction[0].Action != "retry_same_request_id" {
		t.Fatalf("admission reconciliation envelope=%+v err=%v", envelope, err)
	}
}

func TestManagedCleanupIncompleteTerminalReplayRequiresNewPlanAndRequest(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  hostv1.OperationState
		stage  string
		reason string
	}{
		{name: "orphan repaired", state: hostv1.OperationState_OPERATION_STATE_INTERRUPTED, stage: "interrupted", reason: "CLEANUP_TERMINAL_RESULT_UNKNOWN"},
		{name: "host restarted", state: hostv1.OperationState_OPERATION_STATE_INTERRUPTED, stage: "interrupted", reason: "SIDECAR_RESTARTED"},
		{name: "cleanup failed", state: hostv1.OperationState_OPERATION_STATE_FAILED, stage: "failed", reason: "CLEANUP_FAILED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := managedRuntime{
				now: time.Now,
				executeCleanup: func(_ context.Context, _ string, requestID string, _ *hostv1.CleanupPlan, _ []string) (*hostv1.Operation, error) {
					return &hostv1.Operation{
						Id: "operation-1", RequestId: requestID, State: test.state, Stage: test.stage,
						Error: &hostv1.OperationError{Reason: test.reason, Message: "cleanup did not complete; request a new cleanup plan"},
					}, nil
				},
			}
			envelope, err := runtime.cleanup(context.Background(), "execute", managedOptions{
				environmentID: "environment-1",
				cleanupKind:   "reset",
				planRevision:  9,
				requestID:     "cleanup-1",
			})
			issue, _ := envelope.Issue.(managedIssue)
			if err != nil || envelope.State != strings.ToLower(strings.TrimPrefix(test.state.String(), "OPERATION_STATE_")) || !envelope.Partial || issue.Code != test.reason || len(envelope.NextAction) != 1 || envelope.NextAction[0].Action != "cleanup_preview_new_request" {
				t.Fatalf("incomplete cleanup envelope=%+v err=%v", envelope, err)
			}
		})
	}
}

func TestManagedCleanupRejectsUnboundedImpactOutput(t *testing.T) {
	impacts := make([]*hostv1.CleanupImpact, managedCleanupImpactLimit+1)
	for index := range impacts {
		impacts[index] = &hostv1.CleanupImpact{Resource: &hostv1.ResourceReference{Kind: "container", ResourceId: fmt.Sprintf("container-%d", index)}}
	}
	runtime := managedRuntime{now: time.Now, planCleanup: func(context.Context, string, string, hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
		return &hostv1.CleanupPlan{EnvironmentId: "environment-1", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1, Impacts: impacts}, nil
	}}
	var stdout, stderr bytes.Buffer
	if exit := runManagedCLI([]string{"cleanup", "preview", "--environment-id", "environment-1", "--kind", "reset", "--json"}, &stdout, &stderr, runtime); exit != 1 || !strings.Contains(stderr.String(), "limit is 100") {
		t.Fatalf("exit=%d stderr=%s", exit, stderr.String())
	}
}

func TestManagedUpdateKeepsPreparationSeparateFromReplacement(t *testing.T) {
	helperCalls := 0
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			helperCalls++
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			t.Fatalf("replacement ran after preparation failure: %v", args)
			return nil, nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			return "", errors.New("archive is incomplete")
		},
		runHelperInput: func(_ context.Context, _ string, args []string, input []byte) ([]byte, []byte, error) {
			helperCalls++
			recorded := []byte(`{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("a", 64) + `","targetArtifactSize":1024,"stage":"preparation","outcome":"failed","attempt":1,"failureStage":"preparation","failureCode":"update_preparation_failed","failureMessage":"archive is incomplete","retryable":false,"updatedAt":1789372800000}`)
			if args[0] == "operation" {
				return recorded, nil, nil
			}
			if !strings.Contains(strings.Join(args, " "), "update --preparation-failure-stdin") || !strings.Contains(string(input), `"retryable":false`) {
				t.Fatalf("preparation failure was not recorded: args=%v input=%s", args, input)
			}
			return recorded, nil, errors.New("exit status 1")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("a", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || helperCalls != 3 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"disposition":"terminal_action_required"`) || !strings.Contains(stdout.String(), `"stage":"preparation"`) {
		t.Fatalf("exit=%d helperCalls=%d stdout=%s stderr=%s", exit, helperCalls, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateCancellationReportsUnchangedCurrentInstallation(t *testing.T) {
	helperCalls := 0
	runtime := managedRuntime{
		now: func() time.Time { return time.UnixMilli(1789372800000) },
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			helperCalls++
			if args[0] != "status" {
				t.Fatalf("replacement ran after cancellation: %v", args)
			}
			return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		prepareUpdate: func(ctx context.Context, _ managedOptions, _ map[string]any) (string, error) {
			if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) < 19*time.Minute {
				t.Fatalf("update did not receive its long preparation budget: deadline=%v ok=%t", deadline, ok)
			}
			return "", managedUpdateFailure("download", context.Canceled)
		},
		runHelperInput: func(_ context.Context, _ string, args []string, input []byte) ([]byte, []byte, error) {
			helperCalls++
			recorded := []byte(`{"operationId":"operation-cancel","requestId":"update-cancel","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("d", 64) + `","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":1,"failureStage":"download","failureCode":"update_preparation_cancelled","failureMessage":"the update download or preparation was cancelled; the current installation was not changed","retryable":true,"updatedAt":1789372800000}`)
			if args[0] == "operation" {
				return recorded, nil, nil
			}
			if !strings.Contains(strings.Join(args, " "), "--preparation-failure-stdin") || !strings.Contains(string(input), `"retryable":true`) {
				t.Fatalf("cancelled preparation was not recorded: args=%v input=%s", args, input)
			}
			return recorded, nil, errors.New("exit status 1")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-cancel", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("d", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || helperCalls != 3 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"state":"retry_wait"`) || !strings.Contains(stdout.String(), `"disposition":"retry_same_request_at"`) || !strings.Contains(stdout.String(), "current installation was not changed") {
		t.Fatalf("exit=%d helperCalls=%d stdout=%s stderr=%s", exit, helperCalls, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateDoesNotInferDurabilityFromFailedHelperOutput(t *testing.T) {
	digest := strings.Repeat("f", 64)
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] != "status" {
				t.Fatalf("unexpected helper call: %v", args)
			}
			return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			return "", managedRetryableUpdateFailure("download", errors.New("network unavailable"))
		},
		runHelperInput: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, []byte, error) {
			if args[0] == "operation" {
				return []byte(`{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + digest + `","targetArtifactSize":1024,"stage":"download","outcome":"pending","attempt":1,"updatedAt":1789372799999}`), nil, nil
			}
			return []byte(`{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + digest + `","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":1,"failureStage":"download","failureCode":"update_download_unavailable","failureMessage":"network unavailable","retryable":true,"updatedAt":1789372800000}`), nil, errors.New("persistence failed")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", digest, "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"disposition":"terminal_action_required"`) || !strings.Contains(stdout.String(), "update_failure_record_unknown") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateUsesStableOwnerOperationAndLongBudget(t *testing.T) {
	if managedUpdateBudget < 10*time.Minute || managedUpdateBudget <= 15*time.Second {
		t.Fatalf("managed update budget=%s", managedUpdateBudget)
	}
	prepared := "/Users/runner/Library/Application Support/Agent VMM Host/current/.Agent VMM Host.update-1.app"
	removed := ""
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			if strings.Join(args, " ") != "update --source-app "+prepared+" --target-release-id release-2 --request-id update-1 --artifact-sha256 "+strings.Repeat("b", 64)+" --artifact-size 1024" {
				t.Fatalf("update args=%v", args)
			}
			return []byte(`{"operationId":"operation-2","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("b", 64) + `","targetArtifactSize":1024,"stage":"complete","outcome":"succeeded"}`), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) { return prepared, nil },
		removePrepared: func(path string) error {
			removed = path
			return nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("b", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || removed != prepared || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"stage":"complete"`) || !strings.Contains(stdout.String(), `"disposition":"continue_normal_work"`) || !strings.Contains(stdout.String(), `"id":"operation-2"`) {
		t.Fatalf("exit=%d removed=%q stdout=%s stderr=%s", exit, removed, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateRejectsLifecycleResultForDifferentRelease(t *testing.T) {
	prepared := "/Users/runner/Library/Application Support/Agent VMM Host/current/.Agent VMM Host.update-1.app"
	removed := false
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			return []byte(`{"operationId":"operation-other","requestId":"update-1","kind":"update","targetReleaseId":"release-other","stage":"complete","outcome":"succeeded"}`), nil, nil
		},
		prepareUpdate:  func(context.Context, managedOptions, map[string]any) (string, error) { return prepared, nil },
		removePrepared: func(string) error { removed = true; return nil },
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("e", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 1 || removed || !strings.Contains(stderr.String(), "unbound update operation") {
		t.Fatalf("exit=%d removed=%t stdout=%s stderr=%s", exit, removed, stdout.String(), stderr.String())
	}
}

func TestManagedDaemonUpdateRequiresAdministratorBeforeDownload(t *testing.T) {
	prepared := false
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"system/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			prepared = true
			return "", nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("c", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || prepared || !strings.Contains(stdout.String(), `"state":"needs_administrator"`) || !strings.Contains(stdout.String(), `"disposition":"administrator_action_required"`) || !strings.Contains(stdout.String(), "administrator_update_required") {
		t.Fatalf("exit=%d prepared=%t stdout=%s stderr=%s", exit, prepared, stdout.String(), stderr.String())
	}
}

func TestManagedUpdatePreflightHonorsDurableRetryCooldown(t *testing.T) {
	now := time.UnixMilli(1789372800000)
	prepared := false
	digest := strings.Repeat("a", 64)
	runtime := managedRuntime{
		now: func() time.Time { return now },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(fmt.Sprintf(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","operation":{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"%s","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":1,"failureStage":"download","failureCode":"update_download_unavailable","failureMessage":"network unavailable","retryable":true,"updatedAt":%d}}`, digest, now.Add(-10*time.Second).UnixMilli())), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			prepared = true
			return "", nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", digest, "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || prepared || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"state":"retry_wait"`) || !strings.Contains(stdout.String(), `"retry_at":"`) {
		t.Fatalf("exit=%d prepared=%t stdout=%s stderr=%s", exit, prepared, stdout.String(), stderr.String())
	}
}

func TestManagedUpdatePreflightStopsAtDurableRetryLimit(t *testing.T) {
	now := time.UnixMilli(1789372800000)
	prepared := false
	digest := strings.Repeat("b", 64)
	runtime := managedRuntime{
		now: func() time.Time { return now },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(fmt.Sprintf(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","operation":{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"%s","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":3,"failureStage":"download","failureCode":"update_retry_limit_reached","failureMessage":"retry limit reached","retryable":false,"updatedAt":%d}}`, digest, now.Add(-time.Minute).UnixMilli())), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			prepared = true
			return "", nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", digest, "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || prepared || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"disposition":"terminal_action_required"`) || !strings.Contains(stdout.String(), `"max_attempts":3`) {
		t.Fatalf("exit=%d prepared=%t stdout=%s stderr=%s", exit, prepared, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateClassifiesOnlyTransientDownloadFailuresForRetry(t *testing.T) {
	originalClient := managedUpdateHTTPClient
	t.Cleanup(func() { managedUpdateHTTPClient = originalClient })
	root := t.TempDir()
	status := map[string]any{"serviceApp": filepath.Join(root, "Agent VMM Host.app")}
	opts := managedOptions{artifactSize: 1, artifactSHA256: strings.Repeat("a", 64), requestID: "update-1", targetRelease: "release-2"}
	for _, test := range []struct {
		name      string
		status    int
		retryable bool
	}{{name: "server unavailable", status: http.StatusServiceUnavailable, retryable: true}, {name: "missing artifact", status: http.StatusNotFound, retryable: false}} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.status) }))
			defer server.Close()
			managedUpdateHTTPClient = server.Client()
			opts.artifactURL = server.URL
			_, err := prepareManagedUpdate(context.Background(), opts, status)
			var failure managedUpdateError
			if !errors.As(err, &failure) || failure.stage != "download" || failure.retryable != test.retryable {
				t.Fatalf("failure=%+v err=%v", failure, err)
			}
		})
	}
}

func TestLifecycleUpdatePreservesGrantAndSeparatesEndpointApproval(t *testing.T) {
	replacement, clear, invalid := "192.168.1.20:4000", "", "localhost:3000"
	manifest := &appliancev1.ControllerApplianceManifest{RequestedLanEgress: true}
	previous := &appliancev1.ApprovedGrant{Revision: 7, HostTcpEndpoints: []*appliancev1.HostTcpEndpoint{{Ip: "192.168.1.10", Port: 3000}}}
	for _, test := range []struct {
		name      string
		endpoints *string
		approve   bool
		lan       bool
		ip        string
		count     int
	}{
		{"content update", nil, false, false, "192.168.1.10", 1},
		{"ordinary permission approval", nil, true, true, "192.168.1.10", 1},
		{"endpoint replacement", &replacement, false, false, "192.168.1.20", 1},
		{"endpoint clear", &clear, false, false, "", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			grant, err := lifecycleGrant(manifest, previous, test.endpoints, test.approve)
			if err != nil {
				t.Fatal(err)
			}
			if grant.GetLanEgress() != test.lan || len(grant.GetHostTcpEndpoints()) != test.count {
				t.Fatalf("grant=%+v", grant)
			}
			if test.count > 0 && grant.GetHostTcpEndpoints()[0].GetIp() != test.ip {
				t.Fatalf("grant=%+v", grant)
			}
			if !test.approve && grant.GetRevision() != previous.GetRevision() {
				t.Fatal("retained grant revision changed")
			}
			if test.endpoints == nil && !test.approve && !proto.Equal(grant, previous) {
				t.Fatal("content update changed existing grant")
			}
			if previous.GetLanEgress() || previous.GetHostTcpEndpoints()[0].GetIp() != "192.168.1.10" {
				t.Fatal("update mutated current authority")
			}
		})
	}
	if _, err := lifecycleGrant(manifest, previous, &invalid, false); err == nil {
		t.Fatal("hostname endpoint accepted")
	}
}

func TestManagedCleanupRejectionShowsOnlyKnownCodeAndReason(t *testing.T) {
	for _, action := range []string{"preview", "execute"} {
		for _, test := range []struct {
			name           string
			code           codes.Code
			reason         string
			untyped        bool
			expectedReason string
		}{
			{"active_access", codes.FailedPrecondition, "AGENT_ACCESS_ACTIVE", false, "AGENT_ACCESS_ACTIVE"},
			{"unknown_reason", codes.Unavailable, "SECRET_REASON", false, "UNKNOWN"},
			{"untyped", codes.Unknown, "", true, "UNKNOWN"},
			{"unknown_code", codes.Code(99), "ENVIRONMENT_NOT_FOUND", false, "ENVIRONMENT_NOT_FOUND"},
		} {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				ownerError := error(errors.New("SECRET_MESSAGE"))
				if !test.untyped {
					value, err := status.New(test.code, "SECRET_MESSAGE").WithDetails(&errdetails.ErrorInfo{Reason: test.reason, Domain: "SECRET_DOMAIN", Metadata: map[string]string{"resource": "SECRET_RESOURCE"}})
					if err != nil {
						t.Fatal(err)
					}
					ownerError = value.Err()
				}
				runtime := managedRuntime{now: time.Now, planCleanup: func(context.Context, string, string, hostv1.CleanupKind) (*hostv1.CleanupPlan, error) {
					return nil, ownerError
				}, executeCleanup: func(context.Context, string, string, *hostv1.CleanupPlan, []string) (*hostv1.Operation, error) {
					return nil, ownerError
				}}
				options := managedOptions{environmentID: "environment", cleanupKind: "discard", planRevision: 1, requestID: "request"}
				envelope, err := runtime.cleanup(context.Background(), action, options)
				// Untyped execute errors retain the existing unknown-outcome envelope.
				if action == "execute" && test.untyped {
					if err != nil {
						t.Fatal(err)
					}
					body, encodeErr := json.Marshal(envelope)
					if encodeErr != nil || strings.Contains(string(body), "SECRET_") || envelope.State != "unknown_outcome" {
						t.Fatalf("unsafe unknown-outcome envelope=%s %v", body, encodeErr)
					}
					return
				}
				if err == nil {
					t.Fatal("owner rejection disappeared")
				}
				text := err.Error()
				expectedCode := test.code
				if expectedCode > codes.Unauthenticated {
					expectedCode = codes.Unknown
				}
				if !strings.Contains(text, "rpc_code="+expectedCode.String()) || !strings.Contains(text, "reason="+test.expectedReason) || strings.Contains(text, "SECRET_") {
					t.Fatalf("unsafe/missing diagnostic=%q", text)
				}
				if test.reason == "AGENT_ACCESS_ACTIVE" && !strings.Contains(text, "exact agent integration") {
					t.Fatalf("missing actionable ownership diagnostic=%q", text)
				}
			})
		}
	}
}
