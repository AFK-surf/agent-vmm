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
)

func TestDecodeInstallDescriptorRequiresExactHTTPSExchangeRoute(t *testing.T) {
	expiresAt := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	valid := `{"version":1,"operation_id":"operation-1","exchange_url":"https://teams.example.test/v1/compute/agent-vmm/install-tickets/exchange","one_time_secret":"secret","expires_at":"` + expiresAt + `"}`
	if _, err := decodeInstallDescriptor(strings.NewReader(valid)); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}
	wrongRoute := strings.Replace(valid, "/install-tickets/exchange", "/other/exchange", 1)
	if _, err := decodeInstallDescriptor(strings.NewReader(wrongRoute)); err == nil {
		t.Fatal("descriptor outside the install-ticket exchange route was accepted")
	}
	if _, err := decodeInstallDescriptor(strings.NewReader(valid + `{}`)); err == nil {
		t.Fatal("descriptor with trailing JSON was accepted")
	}
}

func TestInstallTicketExchangeAndAcknowledgementUseOneSecret(t *testing.T) {
	previousClient, previousRelease, previousRevision := installHTTPClient, releaseID, targetRevision
	releaseID, targetRevision = "release-1", "7"
	t.Cleanup(func() {
		installHTTPClient, releaseID, targetRevision = previousClient, previousRelease, previousRevision
	})

	material := validTestInstallMaterial()
	var exchangeCalls atomic.Int32
	var ackCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "InstallTicket one-time-secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/v1/compute/agent-vmm/install-tickets/exchange":
			exchangeCalls.Add(1)
			_ = json.NewEncoder(response).Encode(material)
		case "/v1/compute/agent-vmm/install-tickets/ack":
			call := ackCalls.Add(1)
			if call == 1 {
				http.Error(response, "retry", http.StatusServiceUnavailable)
				return
			}
			var body map[string]string
			if json.NewDecoder(request.Body).Decode(&body) != nil || body["operation_id"] != material.OperationID ||
				body["material_digest"] != material.MaterialDigest || body["host_identity_digest"] != material.HostIdentityDigest {
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
	descriptor := installDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-tickets/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}

	exchanged, err := exchangeInstallMaterial(context.Background(), descriptor, []byte(`{"version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateInstallMaterial(exchanged, descriptor); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeInstallMaterial(context.Background(), descriptor, exchanged); err != nil {
		t.Fatal(err)
	}
	if exchangeCalls.Load() != 1 || ackCalls.Load() != 2 {
		t.Fatalf("calls exchange=%d ack=%d, want 1 and 2", exchangeCalls.Load(), ackCalls.Load())
	}
}

func TestInstallTicketAcknowledgementDoesNotRetryPermanentHTTPError(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(response, "invalid material", http.StatusBadRequest)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := installDescriptor{
		Version: 1, OperationID: "operation-1",
		ExchangeURL:   server.URL + "/v1/compute/agent-vmm/install-tickets/exchange",
		OneTimeSecret: "one-time-secret",
	}
	err := acknowledgeInstallMaterial(context.Background(), descriptor, validTestInstallMaterial())
	var responseError installTicketHTTPError
	if !errors.As(err, &responseError) || responseError.status != http.StatusBadRequest || calls.Load() != 1 {
		t.Fatalf("permanent acknowledgement error was retried: err=%v calls=%d", err, calls.Load())
	}
}

func validTestInstallMaterial() installMaterial {
	digest := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	return installMaterial{
		Version: 1, OperationID: "operation-1", RegistrationID: "registration-1", ScopeDigest: digest,
		HostRelease: hostReleaseMaterial{ReleaseID: "release-1", TargetRevision: 7},
		Appliance: applianceMaterial{InstallationID: "installation-1",
			Manifest: base64.StdEncoding.EncodeToString([]byte("manifest")), PublisherSignature: base64.StdEncoding.EncodeToString([]byte("signature")),
			ApprovedGrant: base64.StdEncoding.EncodeToString([]byte("grant")), BootstrapSeed: base64.StdEncoding.EncodeToString([]byte("seed"))},
		RemoteEnrollment: remoteEnrollmentMaterial{GatewayEndpoint: "vmm.example.test:7443", PolicyRevision: 1,
			PoolPolicy: remotePoolMaterial{MaxEnvironments: 1, MaxEgressMode: "deny_all", MaxLeaseSeconds: 60,
				DisconnectGraceSeconds: 1, StopGraceSeconds: 1,
				AggregateLimits:      resourcePolicy{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1},
				PerEnvironmentLimits: resourcePolicy{CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1}}},
		MaterialDigest: digest, HostIdentityDigest: digest,
	}
}

func TestEnsureInstallPlanRetriesDurableAcknowledgementAfterRestart(t *testing.T) {
	previousClient := installHTTPClient
	t.Cleanup(func() { installHTTPClient = previousClient })
	material := validTestInstallMaterial()
	var ackCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/compute/agent-vmm/install-tickets/ack" || request.Header.Get("Authorization") != "InstallTicket one-time-secret" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		ackCalls.Add(1)
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	installHTTPClient = server.Client()
	descriptor := installDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-tickets/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := persistInstallMaterial(directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
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
	var plan persistedInstallPlan
	if json.Unmarshal(data, &plan) != nil || !plan.Acknowledged || plan.AckDescriptor != nil {
		t.Fatalf("acknowledged plan retained acknowledgement capability: %+v", plan)
	}
	if plan.DeadlineUnixMillis <= time.Now().UnixMilli() || adapter.deadlineUnixMillis != plan.DeadlineUnixMillis {
		t.Fatalf("durable install deadline was not preserved: plan=%d adapter=%d", plan.DeadlineUnixMillis, adapter.deadlineUnixMillis)
	}
	if strings.Contains(string(data), "one-time-secret") || ackCalls.Load() != 1 {
		t.Fatalf("ack capability cleanup/calls = %q/%d", data, ackCalls.Load())
	}
}

func TestCompletedInstallReceiptSurvivesSecretPlanCleanup(t *testing.T) {
	previousRelease := releaseID
	releaseID = "release-1"
	t.Cleanup(func() { releaseID = previousRelease })
	value := testPaths(t.TempDir())
	material := validTestInstallMaterial()
	material.HostRelease.Digest = "sha256:host-release-1"
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
		if request.URL.Path != "/v1/compute/agent-vmm/install-tickets/ack" || request.Header.Get("Authorization") != "InstallTicket one-time-secret" {
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
	descriptor := installDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-tickets/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := persistInstallMaterial(directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
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
	var plan persistedInstallPlan
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
	descriptor := installDescriptor{Version: 1, OperationID: material.OperationID,
		ExchangeURL: server.URL + "/v1/compute/agent-vmm/install-tickets/exchange", OneTimeSecret: "one-time-secret",
		ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", material.OperationID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := persistInstallMaterial(directory, material, time.Now().Add(time.Minute).UnixMilli(), false, &descriptor); err != nil {
		t.Fatal(err)
	}
	adapter, err := productionLifecycleAdapterForResume(value, material.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.ensureInstallPlan(context.Background()); err == nil || !terminalInstallTicketError(err) || managedInstallSecretDisposition(err) != secretDispositionDestroyTerminal {
		t.Fatalf("closed handoff did not fail terminally: %v", err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("closed handoff bypassed durable terminal finalization: %v", err)
	}
}
