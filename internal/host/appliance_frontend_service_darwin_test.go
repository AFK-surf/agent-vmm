//go:build darwin

package host

import (
	"context"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestApplianceFrontendServiceDialRejectsStaleDestinationAndRevokedIntent(t *testing.T) {
	for _, change := range []string{"stopped", "replacement_boot", "replacement_instance", "revoked", "expired"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			controller, installation := updateDrainFixture(t, store.ApplianceFrontend{Name: "webhook", Transport: 2, Exposure: 1, MaxFrameBytes: 1024, MaxTotalBytes: 4096, DeadlineSeconds: 10, IdleSeconds: 5, MaxConcurrentStreams: 1})
			installation.State = 3
			installation.DesiredEnabled = true
			installation.CurrentBootID = "boot"
			installation.CurrentContainerInstance = "instance"
			var err error
			installation, err = controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "source", NamespaceID: "source-ns", DisplayName: "source", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
				t.Fatal(err)
			}
			request := &appliancev1.CreateApplianceFrontendServiceImportRequest{RequestId: "approve", InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: "boot", ExpectedContainerInstanceId: "instance", SourceEnvironmentId: "source", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", FrontendName: "webhook", VirtualServiceName: "callback"}
			value := &servicev1.ServiceImport{ImportId: "import", SourceEnvironmentId: "source", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", SourceContainerGenerationId: "gitea-generation", DestinationDeviceId: "device", DestinationFrontend: &servicev1.ApplianceFrontendDestination{InstallationId: installation.ID, Name: "webhook"}, VirtualServiceName: "callback", DestinationPort: 80}
			expires := timestamppb.New(time.Now().Add(time.Hour))
			if change == "expired" {
				expires = timestamppb.New(time.Now().Add(-time.Second))
			}
			route := &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceEnvironmentId: "source", SourceContainerInstanceId: "gitea-instance", DestinationInstallationId: installation.ID, DestinationBootId: "boot", DestinationContainerInstanceId: "instance", DestinationGrantRevision: 1, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires}
			lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: "route", RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{ExpiresAt: expires}}
			if _, err := controller.store.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease); err != nil {
				t.Fatal(err)
			}
			server := &applianceAdminServer{controller: controller}
			listed, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ListApplianceServiceImportsResponse, error) {
				return server.ListServiceImports(ctx, &appliancev1.ListApplianceServiceImportsRequest{InstallationId: installation.ID})
			})
			if err != nil || len(listed.GetImports()) != 1 || listed.Imports[0].SourceContainerGenerationId != "gitea-generation" {
				t.Fatalf("logical import generation projection=%v %v", listed, err)
			}
			if _, err := server.CreateFrontendServiceImport(ctx, request); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("unauthenticated approval=%v", err)
			}
			switch change {
			case "stopped":
				installation.State = 4
			case "replacement_boot":
				installation.CurrentBootID = "replacement"
			case "replacement_instance":
				installation.CurrentContainerInstance = "replacement"
			case "revoked":
				if err := controller.store.RevokeApplianceServiceImport(ctx, &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: "import", ExpectedRevision: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if change == "stopped" || change == "replacement_boot" || change == "replacement_instance" {
				if _, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision); err != nil {
					t.Fatal(err)
				}
			}
			if connection, err := controller.dialApplianceFrontendService(ctx, "route", "lease", 1); err == nil {
				connection.Close()
				t.Fatal("stale callback destination was dialed")
			}
		})
	}
}
