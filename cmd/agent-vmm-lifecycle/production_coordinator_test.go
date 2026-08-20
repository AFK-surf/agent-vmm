package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
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
		"version":                  1,
		"installation_id":          "installation-1",
		"manifest_path":            "manifest.pb",
		"publisher_signature_path": "/tmp/signature",
		"approved_grant_path":      "/tmp/grant.pb",
		"bootstrap_seed_path":      "/tmp/seed",
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

func TestMaterializeInstallTicketUsesPrivateEphemeralWorkspace(t *testing.T) {
	value := testPaths(t.TempDir())
	ticket := map[string]any{
		"version":                    lifecycleInstallTicketVersion,
		"installation_id":            "installation-1",
		"manifest_base64":            base64.StdEncoding.EncodeToString([]byte("manifest")),
		"publisher_signature_base64": base64.StdEncoding.EncodeToString([]byte("signature")),
		"approved_grant_base64":      base64.StdEncoding.EncodeToString([]byte("grant")),
		"bootstrap_seed_base64":      base64.StdEncoding.EncodeToString([]byte("seed")),
	}
	data, err := json.Marshal(ticket)
	if err != nil {
		t.Fatal(err)
	}

	cleanup, configPath, err := materializeInstallTicket(value, "request-1", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if configPath == "" || !strings.HasPrefix(configPath, filepath.Join(value.applicationData, "installation-tickets")) {
		t.Fatalf("config path escaped helper-owned ticket workspace: %q", configPath)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
	cleanup()
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("ticket workspace was not removed: %v", err)
	}
}
