package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDecodeInstallDescriptorRequiresExactHTTPSExchangeRoute(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	valid := `{"version":1,"operation_id":"operation-1","exchange_url":"https://teams.example.test/v1/compute/agent-vmm/install-operations/exchange","one_time_secret":"secret","expires_at":"` + expiresAt + `"}`
	if _, err := decodeOperationHandoffDescriptor(strings.NewReader(valid)); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}
	wrongRoute := strings.Replace(valid, operationHandoffExchangePath, "/other/exchange", 1)
	if _, err := decodeOperationHandoffDescriptor(strings.NewReader(wrongRoute)); err == nil {
		t.Fatal("descriptor outside the operation handoff exchange route was accepted")
	}
	prefixedRoute := strings.Replace(valid, operationHandoffExchangePath, "/proxy"+operationHandoffExchangePath, 1)
	if _, err := decodeOperationHandoffDescriptor(strings.NewReader(prefixedRoute)); err == nil {
		t.Fatal("descriptor with a prefixed operation handoff route was accepted")
	}
	if _, err := decodeOperationHandoffDescriptor(strings.NewReader(valid + `{}`)); err == nil {
		t.Fatal("descriptor with trailing JSON was accepted")
	}
}

func TestManagedInstallExchangeRequestBindsHostIdentity(t *testing.T) {
	body, err := managedInstallExchangeRequest("operation-1", "device-1", []byte("root-key"), 3)
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if json.Unmarshal(body, &request) != nil || request["version"] != float64(2) || request["platform"] != nil ||
		request["operation_id"] != "operation-1" || request["device_id"] != "device-1" ||
		request["root_public_key"] != base64.StdEncoding.EncodeToString([]byte("root-key")) ||
		request["root_key_revision"] != float64(3) {
		t.Fatalf("exchange request = %#v", request)
	}
}

func TestOperationHandoffExchangeAndAcknowledgementUseOneSecret(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })

	material := validTestInstallMaterial()
	var exchangeCalls atomic.Int32
	var ackCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "InstallOperation one-time-secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/v1/compute/agent-vmm/install-operations/exchange":
			exchangeCalls.Add(1)
			_ = json.NewEncoder(response).Encode(material)
		case "/v1/compute/agent-vmm/install-operations/ack":
			call := ackCalls.Add(1)
			if call == 1 {
				http.Error(response, "retry", http.StatusServiceUnavailable)
				return
			}
			var body struct {
				OperationID        string `json:"operation_id"`
				HostIdentityDigest string `json:"host_identity_digest"`
			}
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			if decoder.Decode(&body) != nil || body.OperationID != material.OperationID ||
				body.HostIdentityDigest != material.HostIdentityDigest {
				http.Error(response, "bad acknowledgement", http.StatusBadRequest)
				return
			}
			response.WriteHeader(http.StatusOK)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := operationHandoffDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-operations/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}

	exchanged, err := exchangeOperationHandoff(context.Background(), descriptor, []byte(`{"version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateOperationHandoff(exchanged, descriptor); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeOperationHandoff(context.Background(), descriptor, exchanged); err != nil {
		t.Fatal(err)
	}
	if exchangeCalls.Load() != 1 || ackCalls.Load() != 2 {
		t.Fatalf("calls exchange=%d ack=%d, want 1 and 2", exchangeCalls.Load(), ackCalls.Load())
	}
}

func TestOperationHandoffAcknowledgementDoesNotRetryPermanentHTTPError(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(response, "invalid material", http.StatusBadRequest)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := operationHandoffDescriptor{
		Version: 1, OperationID: "operation-1",
		ExchangeURL:   server.URL + "/v1/compute/agent-vmm/install-operations/exchange",
		OneTimeSecret: "one-time-secret",
	}
	err := acknowledgeOperationHandoff(context.Background(), descriptor, validTestInstallMaterial())
	var responseError operationHandoffHTTPError
	if !errors.As(err, &responseError) || responseError.status != http.StatusBadRequest || calls.Load() != 1 {
		t.Fatalf("permanent acknowledgement error was retried: err=%v calls=%d", err, calls.Load())
	}
}

func validTestInstallMaterial() operationHandoffPlan {
	digest := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	return operationHandoffPlan{
		Version: 1, OperationID: "operation-1", RegistrationID: "registration-1", ScopeDigest: digest,
		Appliance: applianceMaterial{InstallationID: "installation-1",
			ApprovedGrant: base64.StdEncoding.EncodeToString([]byte("grant")), BootstrapSeed: base64.StdEncoding.EncodeToString([]byte("seed"))},
		RemoteEnrollment: remoteEnrollmentMaterial{GatewayEndpoint: "vmm.example.test:7443", PolicyRevision: 1,
			PoolPolicy: remotePoolMaterial{MaxEnvironments: 1, MaxEgressMode: "deny_all", MaxLeaseSeconds: 60,
				DisconnectGraceSeconds: 1, StopGraceSeconds: 1,
				PerEnvironmentLimits: resourcePolicy{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}}},
		HostIdentityDigest: digest,
	}
}

func writeBundledApplianceManifest(t *testing.T, value paths) {
	t.Helper()
	manifestPath := filepath.Join(value.resources, "Guest", "mcp-appliance", "manifest.pb")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte("bundled-manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRootDaemonInstallStateIsServiceUserOwnedAndResumable(t *testing.T) {
	value := testPaths(t.TempDir())
	value.service.Mode = serviceModeDaemon
	value.service.UID, value.service.GID = testServiceIdentity(t)
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	material := validTestInstallMaterial()
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	writeBundledApplianceManifest(t, value)
	if _, err := persistOperationHandoffOwned(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), true, owner); err != nil {
		t.Fatal(err)
	}
	adapter := &productionLifecycleAdapter{value: value, material: &material}
	if err := persistCompletedInstallOwned(value, adapter, owner); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		value.applicationData,
		filepath.Join(value.applicationData, "install-operations"), directory,
		filepath.Join(directory, "manifest.pb"), filepath.Join(directory, "approved-grant.pb"),
		filepath.Join(directory, "bootstrap-seed.bin"), filepath.Join(directory, "appliance.json"), filepath.Join(directory, "plan.json"),
		filepath.Join(value.applicationData, "install-completions"), completedInstallPath(value, material.OperationID),
	}
	for _, path := range paths {
		assertFileOwner(t, path, value.service.UID, value.service.GID)
	}
	if _, err := productionLifecycleAdapterForResume(value, material.OperationID); err != nil {
		t.Fatalf("service-user continuation could not read root-persisted plan: %v", err)
	}
	if err := cleanupInstallPlan(value, material.OperationID); err != nil {
		t.Fatalf("service-user continuation could not clean root-persisted plan: %v", err)
	}
	if completed, err := completedInstallExists(value, material.OperationID); err != nil || !completed {
		t.Fatalf("service-user continuation could not read root-persisted receipt: completed=%t err=%v", completed, err)
	}
}

func TestManagedInstallStateRejectsSymlinkDirectories(t *testing.T) {
	root := t.TempDir()
	value := testPaths(root)
	value.service.Mode = serviceModeDaemon
	value.service.UID = os.Geteuid()
	value.service.GID = os.Getegid()
	owner := lifecycleOperationOwnerFor(value, 0, unix.Fchown)
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(root, "victim")
	if err := os.MkdirAll(victim, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(value.applicationData, "install-operations")); err != nil {
		t.Fatal(err)
	}
	material := validTestInstallMaterial()
	writeBundledApplianceManifest(t, value)
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if _, err := persistOperationHandoffOwned(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), true, owner); err == nil {
		t.Fatal("symlinked install operation directory was accepted")
	}
	if err := cleanupInstallPlan(value, material.OperationID); err == nil {
		t.Fatal("cleanup followed a symlinked install operation root")
	}
	entries, err := os.ReadDir(victim)
	if err != nil || len(entries) != 0 {
		t.Fatalf("symlink target was changed: entries=%v err=%v", entries, err)
	}
	if err := os.Remove(filepath.Join(value.applicationData, "install-operations")); err != nil {
		t.Fatal(err)
	}
	operationDirectory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(operationDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	victimPlan := filepath.Join(victim, "plan.json")
	if err := os.WriteFile(victimPlan, []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victimPlan, filepath.Join(operationDirectory, "plan.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := productionLifecycleAdapterForResume(value, material.OperationID); err == nil {
		t.Fatal("symlinked managed install plan was read")
	}
	if err := cleanupInstallPlan(value, material.OperationID); err != nil {
		t.Fatalf("safe cleanup could not unlink a plan symlink: %v", err)
	}
	if data, err := os.ReadFile(victimPlan); err != nil || string(data) != `{"version":1}` {
		t.Fatalf("safe cleanup changed a plan symlink target: %q err=%v", data, err)
	}
	if err := os.Symlink(victim, filepath.Join(value.applicationData, "install-completions")); err != nil {
		t.Fatal(err)
	}
	adapter := &productionLifecycleAdapter{value: value, material: &material}
	if err := persistCompletedInstallOwned(value, adapter, owner); err == nil {
		t.Fatal("symlinked install completion directory was accepted")
	}
}

func TestManagedInstallStateOwnershipFailureDoesNotPublishPlan(t *testing.T) {
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
	material := validTestInstallMaterial()
	writeBundledApplianceManifest(t, value)
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if _, err := persistOperationHandoffOwned(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), true, owner); err == nil {
		t.Fatal("ownership failure published managed install state")
	}
	if _, err := os.Stat(filepath.Join(directory, "plan.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unowned managed install plan became visible: %v", err)
	}
}

func TestEnsureInstallPlanRetriesDurableAcknowledgementAfterRestart(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	material := validTestInstallMaterial()
	var ackCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/compute/agent-vmm/install-operations/ack" || request.Header.Get("Authorization") != "InstallOperation one-time-secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		ackCalls.Add(1)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := operationHandoffDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-operations/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeBundledApplianceManifest(t, value)
	if _, err := persistOperationHandoff(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
		t.Fatal(err)
	}
	adapter := &productionLifecycleAdapter{value: value, material: &material, descriptor: &descriptor}
	if err := adapter.ensureInstallPlan(context.Background()); err != nil {
		t.Fatalf("durable acknowledgement was not retried: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan persistedInstallOperation
	if json.Unmarshal(data, &plan) != nil || !plan.Acknowledged || plan.AckDescriptor != nil {
		t.Fatalf("acknowledged plan retained acknowledgement capability: %+v", plan)
	}
	if plan.DeadlineUnixMillis <= time.Now().UnixMilli() {
		t.Fatalf("handoff deadline was not preserved for acknowledgement: plan=%d", plan.DeadlineUnixMillis)
	}
	applyContext, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Minute))
	defer cancel()
	if got := adapter.lifecycleDeadline(applyContext); got == plan.DeadlineUnixMillis || got != deadlineFromContext(applyContext) {
		t.Fatalf("local apply deadline = %d, handoff=%d context=%d", got, plan.DeadlineUnixMillis, deadlineFromContext(applyContext))
	}
	if strings.Contains(string(data), "one-time-secret") || ackCalls.Load() != 1 {
		t.Fatalf("ack capability cleanup/calls = %q/%d", data, ackCalls.Load())
	}
}

func deadlineFromContext(ctx context.Context) int64 {
	deadline, _ := ctx.Deadline()
	return deadline.UnixMilli()
}

func TestAcknowledgedExpiredHandoffResumesWithFreshApplyDeadline(t *testing.T) {
	value := testPaths(t.TempDir())
	material := validTestInstallMaterial()
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeBundledApplianceManifest(t, value)
	handoffDeadline := time.Now().Add(-time.Minute).UnixMilli()
	if _, err := persistOperationHandoff(value, directory, material, handoffDeadline, true); err != nil {
		t.Fatal(err)
	}
	adapter, err := productionLifecycleAdapterForResume(value, material.OperationID)
	if err != nil {
		t.Fatalf("expired acknowledged handoff was not resumable: %v", err)
	}
	applyContext, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Minute))
	defer cancel()
	if got := adapter.lifecycleDeadline(applyContext); got <= time.Now().UnixMilli() || got == handoffDeadline || got != deadlineFromContext(applyContext) {
		t.Fatalf("resumed apply deadline = %d, expired handoff=%d context=%d", got, handoffDeadline, deadlineFromContext(applyContext))
	}
}

func TestCompletedInstallReceiptSurvivesSecretPlanCleanup(t *testing.T) {
	value := testPaths(t.TempDir())
	material := validTestInstallMaterial()
	adapter := &productionLifecycleAdapter{value: value, material: &material}
	planDirectory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(planDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(planDirectory, "secret"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := persistCompletedInstall(value, adapter); err != nil {
		t.Fatal(err)
	}
	if err := cleanupInstallPlan(value, material.OperationID); err != nil {
		t.Fatal(err)
	}
	completed, err := completedInstallExists(value, material.OperationID)
	if err != nil || !completed {
		t.Fatalf("completion receipt unavailable after plan cleanup: completed=%t err=%v", completed, err)
	}
	if _, err := os.Stat(planDirectory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret plan was retained: %v", err)
	}
}

func TestFailedManagedLifecycleOperationResumesDurableAcknowledgementWithSameIdentity(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	material := validTestInstallMaterial()
	var ackCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/compute/agent-vmm/install-operations/ack" || request.Header.Get("Authorization") != "InstallOperation one-time-secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		if ackCalls.Add(1) <= 3 {
			http.Error(response, "retry", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := operationHandoffDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-operations/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeBundledApplianceManifest(t, value)
	if _, err := persistOperationHandoff(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
		t.Fatal(err)
	}

	resume := func() error {
		adapter, err := productionLifecycleAdapterForResume(value, material.OperationID)
		if err != nil {
			return err
		}
		return adapter.ensureInstallPlan(context.Background())
	}
	first, err := runManagedLifecycleOperation(value, "install-managed", material.OperationID, resume)
	if err == nil || first.Outcome != "failed" {
		t.Fatalf("first managed acknowledgement=%+v err=%v", first, err)
	}
	second, err := runManagedLifecycleOperation(value, "install-managed", material.OperationID, resume)
	if err != nil {
		t.Fatal(err)
	}
	if second.Outcome != "succeeded" || second.OperationID != first.OperationID || second.RequestID != material.OperationID || ackCalls.Load() != 4 {
		t.Fatalf("managed acknowledgement did not preserve identity: first=%+v second=%+v calls=%d", first, second, ackCalls.Load())
	}
	data, err := os.ReadFile(filepath.Join(directory, "plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan persistedInstallOperation
	if json.Unmarshal(data, &plan) != nil || !plan.Acknowledged || plan.AckDescriptor != nil || bytes.Contains(data, []byte("one-time-secret")) {
		t.Fatalf("acknowledged plan retained capability: %s", data)
	}
}

func TestClosedHandoffMarksTerminalButRetainsPlanUntilFinalized(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	material := validTestInstallMaterial()
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, "closed", http.StatusGone)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := operationHandoffDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-operations/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	writeBundledApplianceManifest(t, value)
	if _, err := persistOperationHandoff(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
		t.Fatal(err)
	}
	adapter, err := productionLifecycleAdapterForResume(value, material.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.ensureInstallPlan(context.Background()); err == nil || !terminalOperationHandoffError(err) || managedInstallSecretDisposition(err) != secretDispositionDestroyTerminal {
		t.Fatalf("closed handoff did not fail terminally: %v", err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("closed handoff bypassed durable terminal finalization: %v", err)
	}
}
