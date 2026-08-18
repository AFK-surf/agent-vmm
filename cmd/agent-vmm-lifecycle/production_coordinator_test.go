package main

import (
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
	t.Setenv(lifecycleApplianceConfigEnv, configPath)

	adapter := &productionLifecycleAdapter{}
	_, err = adapter.readConfig()
	if err == nil || !strings.Contains(err.Error(), "manifest_path") {
		t.Fatalf("relative artifact path was not rejected: %v", err)
	}
}

func TestProductionAdapterRequiresPackagedInstallInputs(t *testing.T) {
	t.Setenv(lifecycleApplianceConfigEnv, "")
	adapter := &productionLifecycleAdapter{}
	if _, err := adapter.installSpec(); err == nil {
		t.Fatal("fresh installation must fail closed without signed inputs")
	}
}
