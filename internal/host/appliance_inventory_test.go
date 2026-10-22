//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type applianceInventoryClient struct {
	vmmdv1.VMMDServiceClient
	appliances []*vmmdv1.SystemAppliance
	listCalls  int
}

func (client *applianceInventoryClient) ListSystemAppliances(_ context.Context, request *vmmdv1.ListSystemAppliancesRequest, _ ...grpc.CallOption) (*vmmdv1.ListSystemAppliancesResponse, error) {
	client.listCalls++
	start := 0
	if request.GetAfterInstallationId() != "" {
		for index, appliance := range client.appliances {
			if appliance.GetIdentity().GetInstallationId() == request.GetAfterInstallationId() {
				start = index + 1
				break
			}
		}
	}
	end := start + int(request.GetLimit())
	if end > len(client.appliances) {
		end = len(client.appliances)
	}
	response := &vmmdv1.ListSystemAppliancesResponse{Appliances: client.appliances[start:end]}
	if end < len(client.appliances) && end > start {
		response.NextCursor = client.appliances[end-1].GetIdentity().GetInstallationId()
	}
	return response, nil
}

func (client *applianceInventoryClient) GetSystemAppliance(_ context.Context, request *vmmdv1.GetSystemApplianceRequest, _ ...grpc.CallOption) (*vmmdv1.SystemApplianceResponse, error) {
	for _, appliance := range client.appliances {
		if appliance.GetIdentity().GetInstallationId() == request.GetInstallationId() {
			return &vmmdv1.SystemApplianceResponse{Appliance: appliance}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func TestListSystemAppliancesConsumesEveryPage(t *testing.T) {
	client := &applianceInventoryClient{}
	for index := range 65 {
		installationID := fmt.Sprintf("installation-%03d", index)
		client.appliances = append(client.appliances, &vmmdv1.SystemAppliance{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: installationID}})
	}
	projection, err := listSystemAppliances(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	if len(projection) != 65 || client.listCalls != 3 {
		t.Fatalf("projection count=%d calls=%d, want 65 across 3 pages", len(projection), client.listCalls)
	}
}

func TestGetSystemApplianceDoesNotScanInventory(t *testing.T) {
	client := &applianceInventoryClient{appliances: []*vmmdv1.SystemAppliance{{Identity: &vmmdv1.SystemApplianceIdentity{InstallationId: "installation-064"}}}}
	appliance, err := getSystemAppliance(context.Background(), client, "installation-064")
	if err != nil || appliance.GetIdentity().GetInstallationId() != "installation-064" || client.listCalls != 0 {
		t.Fatalf("exact appliance=%+v err=%v list calls=%d", appliance, err, client.listCalls)
	}
	missing, err := getSystemAppliance(context.Background(), client, "installation-missing")
	if err != nil || missing != nil {
		t.Fatalf("missing appliance=%+v err=%v", missing, err)
	}
}

func TestApplianceSourcePolicyFollowsPersistedInstanceAndRecovery(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	_, _, err = database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "artifact", ProductID: "product", CanonicalHash: []byte("hash"), ImageDigest: "digest", ImageReference: "image", Platform: "linux/arm64", Release: "1", Manifest: []byte("{}")}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), Revision: 1, DataVolumeID: "volume", DataVolumeBytes: 1024}, store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, LANEgress: true, EgressMode: 1, MaxEgressMode: 1, HostTCPEndpoints: store.HostTCPEndpoints{netip.MustParseAddrPort("192.168.88.16:3000")}}, store.RemoteRegistration{ID: "registration", InstallationID: "installation", OwnerKind: store.RemoteRegistrationPrivate, DisplayName: "private", ProtocolVersion: "1", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1}, nil, store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: installation.ID, BootId: "boot", NamespaceId: systemNamespaceID(installation.ID), ContainerInstanceId: "instance"}
	inventory := &applianceInventoryClient{appliances: []*vmmdv1.SystemAppliance{{Identity: identity, NetworkSubnet: "10.77.0.0/24", State: vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING}}}
	policies := new(egress.Policies)
	revision := uint64(0)
	publish := func(expected bool) egress.Source {
		t.Helper()
		sources, err := controller.applianceEgressSources(ctx, inventory, "boot")
		if err != nil || len(sources) != 1 {
			t.Fatalf("source count=%d error=%v", len(sources), err)
		}
		revision++
		if err := policies.Publish(egress.Snapshot{BootID: "boot", Revision: revision, Sources: sources}); err != nil {
			t.Fatal(err)
		}
		source, allowed := policies.Resolve("boot", netip.MustParseAddr("10.77.0.2"))
		if allowed != expected {
			t.Fatalf("allowed=%t want=%t source=%+v", allowed, expected, source)
		}
		return source
	}
	publish(false)
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
	installation.CurrentBootID = "boot"
	installation.CurrentContainerInstance = "instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	source := publish(true)
	if !source.LANEgress || source.InstanceID != "instance" || source.GrantRevision != 1 || len(source.HostTCPEndpoints) != 1 {
		t.Fatalf("persisted source=%+v", source)
	}
	flow, cancel := policies.Context(context.Background(), "boot", netip.MustParseAddr("10.77.0.2"), source)
	defer cancel()
	recovered := &Controller{store: database}
	controller = recovered
	publish(true)
	select {
	case <-flow.Done():
		t.Fatal("recovery canceled unchanged instance")
	default:
	}
	installation.CurrentContainerInstance = "replacement"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	publish(false)
	select {
	case <-flow.Done():
	case <-time.After(time.Second):
		t.Fatal("replacement retained old connection")
	}
	identity.ContainerInstanceId = "replacement"
	publish(true)
	installation.DesiredEnabled = false
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	publish(false)
}
