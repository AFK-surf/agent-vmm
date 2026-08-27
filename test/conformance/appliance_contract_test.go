package conformance

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

type resources struct {
	CPU    uint64 `json:"cpu_millis"`
	Memory uint64 `json:"memory_bytes"`
	PIDs   uint64 `json:"pids"`
	Disk   uint64 `json:"disk_bytes"`
}

func (r resources) add(other resources) resources {
	return resources{CPU: r.CPU + other.CPU, Memory: r.Memory + other.Memory, PIDs: r.PIDs + other.PIDs, Disk: r.Disk + other.Disk}
}

func (r resources) fits(limit resources) bool {
	return r.CPU <= limit.CPU && r.Memory <= limit.Memory && r.PIDs <= limit.PIDs && r.Disk <= limit.Disk
}

type applianceFixture struct {
	Version               uint64 `json:"version"`
	RequiredRemoteFeature string `json:"required_remote_feature"`
	Limits                struct {
		ManifestBytes              uint64 `json:"manifest_bytes"`
		BootstrapBytes             uint64 `json:"bootstrap_bytes"`
		BootstrapFrameBytes        uint64 `json:"bootstrap_frame_bytes"`
		BootstrapTotalBytes        uint64 `json:"bootstrap_total_bytes"`
		BootstrapDeadlineSeconds   uint64 `json:"bootstrap_deadline_seconds"`
		BootstrapIdleSeconds       uint64 `json:"bootstrap_idle_seconds"`
		BootstrapConcurrentStreams uint64 `json:"bootstrap_concurrent_streams"`
		BootstrapUseCount          uint64 `json:"bootstrap_use_count"`
		FrontendFrameBytes         uint64 `json:"frontend_frame_bytes"`
		FrontendTotalBytes         uint64 `json:"frontend_total_bytes"`
		FrontendIdleSeconds        uint64 `json:"frontend_idle_seconds"`
		FrontendConcurrentStreams  uint64 `json:"frontend_concurrent_streams"`
		FrontendsPerInstallation   uint64 `json:"frontends_per_installation"`
		DynamicFrontendLeases      uint64 `json:"dynamic_frontend_leases"`
		DynamicLeasesPerAllocation uint64 `json:"dynamic_leases_per_allocation"`
		DynamicFrontendConnections uint64 `json:"dynamic_frontend_connections"`
		DynamicFrontendUseCount    uint64 `json:"dynamic_frontend_use_count"`
		BootstrapNonceBytes        uint64 `json:"bootstrap_nonce_bytes"`
		ReadinessNonceBytes        uint64 `json:"readiness_nonce_bytes"`
		HealthStatusBytes          uint64 `json:"health_status_bytes"`
		BoundedLogBytes            uint64 `json:"bounded_log_bytes"`
		DataVolumeMinBytes         uint64 `json:"data_volume_min_bytes"`
		DataVolumeMaxBytes         uint64 `json:"data_volume_max_bytes"`
	} `json:"limits"`
	Host struct {
		resources
		Reserve resources `json:"reserve"`
	} `json:"host"`
	Domains []struct {
		InstallationID        string    `json:"installation_id"`
		RegistrationID        string    `json:"registration_id"`
		ArtifactID            string    `json:"artifact_id"`
		ConnectionEpochs      []uint64  `json:"connection_epochs"`
		InventoryWatermarks   []uint64  `json:"inventory_watermarks"`
		SystemNamespace       string    `json:"system_namespace"`
		SystemContainer       string    `json:"system_container"`
		SystemVolume          string    `json:"system_volume"`
		SystemReservation     resources `json:"system_reservation"`
		AllocationReservation resources `json:"allocation_reservation"`
		DomainCeiling         resources `json:"domain_ceiling"`
		Frontend              struct {
			Name, Transport, Exposure, Endpoint string
		} `json:"frontend"`
		BootstrapSocket  string `json:"bootstrap_socket"`
		ControllerSocket string `json:"controller_socket"`
		UID              uint32 `json:"uid"`
		Runtime          string `json:"runtime"`
	} `json:"domains"`
	ExternalRegistrationID string `json:"external_registration_id"`
	BundledMCP             struct {
		ProductID        string   `json:"product_id"`
		Platform         string   `json:"platform"`
		DigestPinned     bool     `json:"digest_pinned"`
		DefaultEnabled   bool     `json:"default_enabled"`
		AgentAllocations []string `json:"agent_allocations"`
	} `json:"bundled_mcp"`
}

func loadApplianceFixture(t *testing.T) applianceFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/appliance-contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture applianceFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestApplianceContractIdentitiesAndIsolation(t *testing.T) {
	fixture := loadApplianceFixture(t)
	if fixture.Version != 1 || fixture.RequiredRemoteFeature != "controller-ready-v1" || len(fixture.Domains) != 2 {
		t.Fatal("contract must freeze two domains")
	}
	installations, registrations, namespaces, containers, volumes, endpoints := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, domain := range fixture.Domains {
		for name, value := range map[string]string{"installation": domain.InstallationID, "registration": domain.RegistrationID, "namespace": domain.SystemNamespace, "container": domain.SystemContainer, "volume": domain.SystemVolume, "frontend": domain.Frontend.Endpoint} {
			if value == "" {
				t.Fatalf("%s identity is empty", name)
			}
		}
		for value, set := range map[string]map[string]bool{domain.InstallationID: installations, domain.RegistrationID: registrations, domain.SystemNamespace: namespaces, domain.SystemContainer: containers, domain.SystemVolume: volumes, domain.Frontend.Endpoint: endpoints} {
			if set[value] {
				t.Fatalf("identity %q is shared across domains", value)
			}
			set[value] = true
		}
		if domain.RegistrationID == fixture.ExternalRegistrationID || domain.ArtifactID == "" || domain.UID != 1000 || domain.Runtime != "runsc" {
			t.Fatalf("invalid domain identity or sandbox: %+v", domain)
		}
		if domain.BootstrapSocket != "/run/agent-vmm/bootstrap.sock" || domain.ControllerSocket != "/run/agent-vmm/controller.sock" {
			t.Fatal("private socket contract changed")
		}
		if !domain.SystemReservation.add(domain.AllocationReservation).fits(domain.DomainCeiling) {
			t.Fatalf("domain %q exceeds its ceiling", domain.InstallationID)
		}
		for i, watermark := range domain.InventoryWatermarks {
			if watermark == 0 || i > 0 && watermark <= domain.InventoryWatermarks[i-1] {
				t.Fatalf("registration watermark is not strictly monotonic: %v", domain.InventoryWatermarks)
			}
		}
		for i, epoch := range domain.ConnectionEpochs {
			if epoch == 0 || i > 0 && epoch <= domain.ConnectionEpochs[i-1] {
				t.Fatalf("connection epoch is not strictly monotonic: %v", domain.ConnectionEpochs)
			}
		}
	}
}

func TestApplianceAggregateAdmissionAndBounds(t *testing.T) {
	fixture := loadApplianceFixture(t)
	used := fixture.Host.Reserve
	for _, domain := range fixture.Domains {
		used = used.add(domain.SystemReservation).add(domain.AllocationReservation)
	}
	if !used.fits(fixture.Host.resources) {
		t.Fatalf("aggregate reservation %+v exceeds host %+v", used, fixture.Host.resources)
	}
	limits := fixture.Limits
	if limits.ManifestBytes == 0 || limits.BootstrapBytes == 0 || limits.BootstrapFrameBytes > limits.BootstrapTotalBytes || limits.BootstrapBytes > limits.BootstrapTotalBytes || limits.BootstrapConcurrentStreams != 1 || limits.BootstrapUseCount != 1 {
		t.Fatal("bootstrap contract is not bounded and single-use")
	}
	if limits.BootstrapDeadlineSeconds == 0 || limits.BootstrapIdleSeconds == 0 || limits.FrontendFrameBytes == 0 || limits.FrontendTotalBytes == 0 || limits.FrontendIdleSeconds == 0 || limits.FrontendConcurrentStreams == 0 || limits.FrontendsPerInstallation == 0 {
		t.Fatal("stream bounds must be positive")
	}
	if limits.DynamicFrontendLeases == 0 || limits.DynamicLeasesPerAllocation == 0 || limits.DynamicLeasesPerAllocation > limits.DynamicFrontendLeases || limits.DynamicFrontendConnections == 0 || limits.DynamicFrontendUseCount == 0 {
		t.Fatal("dynamic frontend capacity must be positive and allocation-scoped")
	}
	if limits.BootstrapNonceBytes != 32 || limits.ReadinessNonceBytes != 32 || limits.HealthStatusBytes == 0 || limits.BoundedLogBytes == 0 || limits.DataVolumeMinBytes == 0 || limits.DataVolumeMaxBytes < limits.DataVolumeMinBytes {
		t.Fatal("artifact and health bounds changed")
	}
}

func TestBundledMCPContract(t *testing.T) {
	fixture := loadApplianceFixture(t)
	mcp := fixture.BundledMCP
	if mcp.ProductID == "" || mcp.Platform != "linux/arm64" || !mcp.DigestPinned || mcp.DefaultEnabled {
		t.Fatal("bundled MCP artifact must be digest-pinned, arm64, and disabled by default")
	}
	if len(mcp.AgentAllocations) < 2 || mcp.AgentAllocations[0] == mcp.AgentAllocations[1] {
		t.Fatal("each local agent must have a distinct allocation")
	}
}

func TestControllerReadyContract(t *testing.T) {
	tests := []struct {
		name         string
		ready        *remotev1.ControllerReady
		registration string
		epoch        uint64
		watermark    uint64
		wantErr      bool
	}{
		{name: "exact snapshot", ready: &remotev1.ControllerReady{RegistrationId: "r", ConnectionEpoch: 4, AcknowledgedInventoryWatermark: 9}, registration: "r", epoch: 4, watermark: 9},
		{name: "empty inventory still has revision", ready: &remotev1.ControllerReady{RegistrationId: "empty", ConnectionEpoch: 1, AcknowledgedInventoryWatermark: 1}, registration: "empty", epoch: 1, watermark: 1},
		{name: "missing", wantErr: true},
		{name: "cross registration", ready: &remotev1.ControllerReady{RegistrationId: "other", ConnectionEpoch: 4, AcknowledgedInventoryWatermark: 9}, registration: "r", epoch: 4, watermark: 9, wantErr: true},
		{name: "stale epoch", ready: &remotev1.ControllerReady{RegistrationId: "r", ConnectionEpoch: 3, AcknowledgedInventoryWatermark: 9}, registration: "r", epoch: 4, watermark: 9, wantErr: true},
		{name: "stale watermark", ready: &remotev1.ControllerReady{RegistrationId: "r", ConnectionEpoch: 4, AcknowledgedInventoryWatermark: 8}, registration: "r", epoch: 4, watermark: 9, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateControllerReady(test.ready, test.registration, test.epoch, test.watermark)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func validateControllerReady(ready *remotev1.ControllerReady, registration string, epoch, watermark uint64) error {
	if ready == nil || ready.GetRegistrationId() != registration {
		return errors.New("registration mismatch")
	}
	if epoch == 0 || ready.GetConnectionEpoch() != epoch {
		return errors.New("connection epoch mismatch")
	}
	if watermark == 0 || ready.GetAcknowledgedInventoryWatermark() != watermark {
		return errors.New("inventory watermark mismatch")
	}
	return nil
}

func TestInventoryRevisionAdvancesForEveryMutation(t *testing.T) {
	type snapshot struct {
		watermark           uint64
		allocationRevisions []uint64
	}
	snapshots := []snapshot{
		{watermark: 1},
		{watermark: 2, allocationRevisions: []uint64{1, 1}},
		{watermark: 3, allocationRevisions: []uint64{2, 1}},
		{watermark: 4, allocationRevisions: []uint64{2}},
		{watermark: 5, allocationRevisions: []uint64{2}},
	}
	for i := 1; i < len(snapshots); i++ {
		if snapshots[i].watermark <= snapshots[i-1].watermark {
			t.Fatal("authoritative inventory revision did not advance")
		}
	}
	if snapshots[3].allocationRevisions[0] != snapshots[4].allocationRevisions[0] {
		t.Fatal("restart normalization fixture changed allocation revision")
	}
}
