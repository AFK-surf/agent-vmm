//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func TestServiceIdentitySurvivesStoreAndAgentWire(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, id := range []string{"service", "neighbor"} {
		if _, err := database.CreateEnvironment(ctx, store.Environment{ID: id, NamespaceID: id, DisplayName: id, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
			t.Fatal(err)
		}
	}
	exported, err := database.CreateServiceExport(ctx, "export-request", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "service", ContainerId: "gitea", ContainerGenerationId: "generation", ContainerInstanceId: "instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateServiceExport(ctx, "neighbor-request", &servicev1.ServiceExport{ExportId: "neighbor", EnvironmentId: "neighbor", ContainerId: "gitea", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
		t.Fatal(err)
	}
	capability := servicenet.LocalServiceRouteCapability("import-request", "device", "allocation", "export", true)
	imported, err := database.CreateServiceImport(ctx, "import-request", &servicev1.ServiceImport{ImportId: "import", SourceEnvironmentId: "service", SourceContainerId: "controller", SourceContainerInstanceId: "controller-instance", DestinationDeviceId: "device", DestinationExportId: "export", VirtualServiceName: "gitea", DestinationPort: 3000}, &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_LOCAL, SourceDeviceId: "device", SourceAllocationId: proto.String("allocation"), DestinationDeviceId: "device", DestinationExportId: "export", State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: capability.GetExpiresAt(), SourceEnvironmentId: "service", SourceContainerInstanceId: "controller-instance", PolicyRevision: 1}, &servicev1.RouteLease{LeaseId: "lease", RouteId: "route", RouteRevision: 1, Generation: capability.GetRouteGeneration(), ExpiresAt: capability.GetExpiresAt(), Capability: capability})
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"first", "reconnected"} {
		client := executionClientWithIdentityAndServer(t, auth.Identity{Role: auth.Agent, SessionID: session, EnvironmentID: "service", NamespaceID: "service"}, &agentServer{controller: newTestController(&Controller{store: database})})
		exports, err := client.ListServiceExports(ctx, &hostv1.ListServiceExportsRequest{})
		if err != nil || len(exports.GetExports().GetExports()) != 1 || !proto.Equal(exports.GetExports().GetExports()[0], exported) {
			t.Fatalf("export list: %v %v", exports, err)
		}
		imports, err := client.ListServiceImports(ctx, &hostv1.ListServiceImportsRequest{})
		if err != nil || len(imports.GetImports()) != 1 || !proto.Equal(imports.GetImports()[0], imported) {
			t.Fatalf("import list: %v %v", imports, err)
		}

	}
}
