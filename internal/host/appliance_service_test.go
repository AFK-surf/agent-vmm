//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestApplianceServiceApprovalReplayNeverReprojectsRevokedIntent(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	installation.State = 3
	installation.DesiredEnabled = true
	installation.CurrentBootID = "boot"
	installation.CurrentContainerInstance = "instance"
	installation, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	exported, err := controller.store.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "gitea", ContainerId: "gitea", ContainerInstanceId: "gitea-instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	request := &appliancev1.CreateApplianceServiceImportRequest{RequestId: "approve", InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: "boot", ExpectedContainerInstanceId: "instance", DestinationExportId: exported.ExportId, ExpectedExportRevision: 1, VirtualServiceName: "gitea"}
	value := &servicev1.ServiceImport{ImportId: "import", SourceInstallationId: installation.ID, SourceContainerId: "controller", DestinationDeviceId: "device", DestinationExportId: exported.ExportId, DestinationPort: 3000, VirtualServiceName: "gitea"}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	route := &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires, SourceInstallationId: installation.ID, SourceBootId: "boot", SourceContainerInstanceId: "instance", PolicyRevision: 1, DestinationExportId: exported.ExportId}
	lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: route.RouteId, RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{CapabilityId: "capability", ExpiresAt: expires}}
	if _, err := controller.store.CreateApplianceServiceImport(ctx, request, "system-fixture", value, route, lease); err != nil {
		t.Fatal(err)
	}
	_, denied := auth.UnaryInterceptor(auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "gitea", NamespaceID: "gitea-ns"})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return (&agentServer{controller: controller}).RevokeServiceImport(ctx, &hostv1.RevokeServiceImportRequest{RequestId: "unauthorized", ImportId: value.ImportId, ExpectedRevision: 1})
	})
	if status.Code(denied) != codes.Aborted {
		t.Fatalf("ordinary agent revoked appliance projection=%v", denied)
	}
	if err := controller.store.RevokeApplianceServiceImport(ctx, &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: value.ImportId, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	// A replacement source instance must not prevent observation of the original approval receipt.
	installation.CurrentBootID = "replacement-boot"
	installation.CurrentContainerInstance = "replacement-instance"
	if _, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision); err != nil {
		t.Fatal(err)
	}
	server := controller.appliances
	replay, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.CreateApplianceServiceImportResponse, error) {
		return server.CreateApplianceServiceImport(ctx, request)
	})
	if err != nil || !replay.GetImport().Revoked || replay.GetImport().ImportId != value.ImportId {
		t.Fatalf("replay=%v %v", replay, err)
	}

	// The locked receipt lookup proves these unsent stale requests were never admitted.
	unsent := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
	unsent.RequestId = "unsent-service"
	_, rejected := callAsOperator(t, func(ctx context.Context) (*appliancev1.CreateApplianceServiceImportResponse, error) {
		return server.CreateApplianceServiceImport(ctx, unsent)
	})
	callback := &appliancev1.CreateApplianceFrontendServiceImportRequest{RequestId: "unsent-callback", InstallationId: installation.ID, ExpectedRevision: request.ExpectedRevision, ExpectedGrantRevision: request.ExpectedGrantRevision, ExpectedBootId: request.ExpectedBootId, ExpectedContainerInstanceId: request.ExpectedContainerInstanceId, SourceEnvironmentId: "gitea", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", FrontendName: "webhook", VirtualServiceName: "callback"}
	_, callbackRejected := callAsOperator(t, func(ctx context.Context) (*appliancev1.CreateApplianceFrontendServiceImportResponse, error) {
		return server.CreateApplianceFrontendServiceImport(ctx, callback)
	})
	for _, failure := range []error{rejected, callbackRejected} {
		if status.Code(failure) != codes.Aborted {
			t.Fatalf("stale admission=%v", failure)
		}

	}

	if _, err := controller.store.ReplayApplianceServiceImport(ctx, unsent); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale service request accepted: %v", err)
	}
	if _, err := controller.store.ReplayApplianceFrontendServiceImport(ctx, callback); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale frontend request accepted: %v", err)
	}

	for _, mutate := range []func(*appliancev1.CreateApplianceServiceImportRequest){func(r *appliancev1.CreateApplianceServiceImportRequest) { r.InstallationId = "neighbor" }, func(r *appliancev1.CreateApplianceServiceImportRequest) { r.VirtualServiceName = "neighbor" }} {
		changed := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
		mutate(changed)
		_, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.CreateApplianceServiceImportResponse, error) {
			return server.CreateApplianceServiceImport(ctx, changed)
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("changed owner/payload=%v", err)
		}
	}
}
