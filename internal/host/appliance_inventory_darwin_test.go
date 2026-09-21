//go:build darwin

package host

import (
	"context"
	"fmt"
	"testing"

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
