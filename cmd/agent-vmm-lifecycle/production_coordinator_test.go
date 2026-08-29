package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func TestApplianceReadyRequiresRegistrationAndCurrentControllerFacts(t *testing.T) {
	base := &appliancev1.ApplianceStatus{
		RegistrationId:     "registration-1",
		Enabled:            true,
		ApplianceState:     appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING,
		ChannelState:       appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		InventoryWatermark: 1,
		Health: &appliancev1.ApplianceHealth{
			Healthy: true,
		},
	}

	if !applianceReady(base) {
		t.Fatal("complete appliance facts should be ready")
	}

	checks := []func(*appliancev1.ApplianceStatus){
		func(status *appliancev1.ApplianceStatus) { status.RegistrationId = "" },
		func(status *appliancev1.ApplianceStatus) { status.Enabled = false },
		func(status *appliancev1.ApplianceStatus) {
			status.ApplianceState = appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED
		},
		func(status *appliancev1.ApplianceStatus) {
			status.ChannelState = appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_DISCONNECTED
		},
		func(status *appliancev1.ApplianceStatus) { status.InventoryWatermark = 0 },
		func(status *appliancev1.ApplianceStatus) { status.Health.Healthy = false },
	}
	for index, check := range checks {
		status := proto.Clone(base).(*appliancev1.ApplianceStatus)
		check(status)
		if applianceReady(status) {
			t.Fatalf("check %d incorrectly reported ready: %+v", index, status)
		}
	}
}

func TestReadConfigFailsClosedForRelativeArtifactPaths(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "appliance.json")
	config := map[string]any{
		"version":             1,
		"installation_id":     "installation-1",
		"manifest_path":       "manifest.pb",
		"approved_grant_path": "/tmp/grant.pb",
		"bootstrap_seed_path": "/tmp/seed",
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	adapter := &productionLifecycleAdapter{configPath: configPath}
	_, err = adapter.readConfig()
	if err == nil || !strings.Contains(err.Error(), "manifest_path") {
		t.Fatalf("relative artifact path was not rejected: %v", err)
	}
}

func TestProductionAdapterRequiresPackagedInstallInputs(t *testing.T) {
	adapter := &productionLifecycleAdapter{}
	if _, err := adapter.installSpec(); err == nil {
		t.Fatal("fresh installation must fail closed without signed inputs")
	}
}

func TestExistingApplianceMustMatchRequestedArtifact(t *testing.T) {
	cpu, pids := uint32(1), uint32(1)
	memory, disk := uint64(1), uint64(1)
	limits := &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
	grant := &appliancev1.ApprovedGrant{SystemResources: &appliancev1.ResourceRange{Floor: proto.Clone(limits).(*hostv1.ResourceLimits), Ceiling: proto.Clone(limits).(*hostv1.ResourceLimits)},
		Capacity: &appliancev1.CapacityGrant{MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL}
	status := &appliancev1.ApplianceStatus{RegistrationId: "registration-1", ArtifactId: "artifact-1", ApprovedGrant: proto.Clone(grant).(*appliancev1.ApprovedGrant)}
	if err := validateExistingAppliance(status, "artifact-1", grant); err != nil {
		t.Fatalf("exact existing artifact was rejected: %v", err)
	}
	mismatch := validateExistingAppliance(status, "artifact-2", grant)
	if mismatch == nil || !strings.Contains(mismatch.Error(), "artifact") {
		t.Fatalf("mismatched existing artifact was accepted: %v", mismatch)
	}
	value := testPaths(t.TempDir())
	calls := 0
	operation, operationErr := runManagedLifecycleOperation(value, "install-managed", "operation-artifact-mismatch", func() error {
		calls++
		return mismatch
	})
	if operationErr == nil || calls != 1 || operation.Retryable || operation.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("artifact mismatch did not produce a terminal report: operation=%+v calls=%d err=%v", operation, calls, operationErr)
	}
	resumed, resumeErr := runManagedLifecycleOperation(value, "install-managed", "operation-artifact-mismatch", func() error {
		calls++
		return nil
	})
	if resumeErr == nil || calls != 1 || resumed.SecretDisposition != secretDispositionDestroyTerminal {
		t.Fatalf("terminal artifact mismatch was replayed or lost: operation=%+v calls=%d err=%v", resumed, calls, resumeErr)
	}
	changedGrant := proto.Clone(grant).(*appliancev1.ApprovedGrant)
	changedGrant.Capacity.MaxEnvironments = 1
	if err := validateExistingAppliance(status, "artifact-1", changedGrant); err == nil || !strings.Contains(err.Error(), "grant") {
		t.Fatalf("mismatched existing grant was accepted: %v", err)
	}
}

func TestLocalApplyRequestIdentityIncludesFreshDeadline(t *testing.T) {
	first := lifecycleApplyRequestID("operation-1", "stage", 100)
	if first != lifecycleApplyRequestID("operation-1", "stage", 100) {
		t.Fatal("same local apply fence changed request identity")
	}
	if first == lifecycleApplyRequestID("operation-1", "stage", 101) {
		t.Fatal("fresh local apply deadline reused the old request identity")
	}
}

func TestPersistInstallMaterialUsesPrivateDurableWorkspace(t *testing.T) {
	value := testPaths(t.TempDir())
	directory := filepath.Join(value.applicationData, "install-operations", "operation-1")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	material := operationHandoffPlan{Version: 1, OperationID: "operation-1", Appliance: applianceMaterial{InstallationID: "installation-1",
		ApprovedGrant: base64.StdEncoding.EncodeToString([]byte("grant")), BootstrapSeed: base64.StdEncoding.EncodeToString([]byte("seed"))}}
	writeBundledApplianceManifest(t, value)
	configPath, err := persistOperationHandoff(value, directory, material, time.Now().Add(time.Minute).UnixMilli(), true)
	if err != nil {
		t.Fatal(err)
	}
	if configPath == "" || !strings.HasPrefix(configPath, filepath.Join(value.applicationData, "install-operations", "operation-1")) {
		t.Fatalf("config path escaped helper-owned operation workspace: %q", configPath)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
	manifest, err := os.ReadFile(filepath.Join(directory, "manifest.pb"))
	if err != nil || string(manifest) != "bundled-manifest" {
		t.Fatalf("persisted manifest=%q err=%v", manifest, err)
	}
	plan, err := os.ReadFile(filepath.Join(directory, "plan.json"))
	if err != nil || strings.Contains(string(plan), `"manifest"`) {
		t.Fatalf("operation plan retained a Server manifest: %s err=%v", plan, err)
	}
	if _, err := newProductionLifecycleAdapterForResume(value, "operation-1"); err != nil {
		t.Fatalf("acknowledged plan was not resumable: %v", err)
	}
	if err := cleanupInstallPlan(value, "operation-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("operation workspace was not removed: %v", err)
	}
}
