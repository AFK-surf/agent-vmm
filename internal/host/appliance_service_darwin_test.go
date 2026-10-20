//go:build darwin

package host

import (
	"context"
	"errors"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestApplianceServiceApprovalReplayIsOperatorOnlyAndNeverReprojectsRevokedIntent(t *testing.T) {
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
	if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", CPUMillis: 1, MemoryBytes: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	exported, err := controller.store.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "gitea", ContainerId: "gitea", ContainerInstanceId: "gitea-instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	request := &appliancev1.CreateApplianceServiceImportRequest{RequestId: "approve", InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: "boot", ExpectedContainerInstanceId: "instance", DestinationExportId: exported.ExportId, ExpectedExportRevision: 1, VirtualServiceName: "gitea"}
	value := &servicev1.ServiceImport{ImportId: "import", SourceInstallationId: installation.ID, SourceContainerId: systemContainerID(), DestinationDeviceId: "device", DestinationExportId: exported.ExportId, DestinationPort: 3000, VirtualServiceName: "gitea"}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	route := &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires, SourceInstallationId: installation.ID, SourceBootId: "boot", SourceContainerInstanceId: "instance", PolicyRevision: 1, DestinationExportId: exported.ExportId}
	lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: route.RouteId, RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{CapabilityId: "capability", ExpiresAt: expires}}
	if _, err := controller.store.CreateApplianceServiceImport(ctx, request, systemNamespaceID(installation.ID), value, route, lease); err != nil {
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
	server := &applianceAdminServer{controller: controller}
	if _, err := server.CreateServiceImport(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unauthenticated replay=%v", err)
	}
	replay, err := callAsOperator(t, func(ctx context.Context) (*servicev1.ServiceImport, error) {
		return server.CreateServiceImport(ctx, request)
	})
	if err != nil || !replay.Revoked || replay.ImportId != value.ImportId {
		t.Fatalf("replay=%v %v", replay, err)
	}

	// The locked receipt lookup proves these unsent stale requests were never admitted.
	unsent := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
	unsent.RequestId = "unsent-service"
	_, rejected := callAsOperator(t, func(ctx context.Context) (*servicev1.ServiceImport, error) {
		return server.CreateServiceImport(ctx, unsent)
	})
	callback := &appliancev1.CreateApplianceFrontendServiceImportRequest{RequestId: "unsent-callback", InstallationId: installation.ID, ExpectedRevision: request.ExpectedRevision, ExpectedGrantRevision: request.ExpectedGrantRevision, ExpectedBootId: request.ExpectedBootId, ExpectedContainerInstanceId: request.ExpectedContainerInstanceId, SourceEnvironmentId: "gitea", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", FrontendName: "webhook", VirtualServiceName: "callback"}
	_, callbackRejected := callAsOperator(t, func(ctx context.Context) (*servicev1.ServiceImport, error) {
		return server.CreateFrontendServiceImport(ctx, callback)
	})
	for _, failure := range []error{rejected, callbackRejected} {
		if status.Code(failure) != codes.Aborted {
			t.Fatalf("stale admission=%v", failure)
		}
		proved := false
		for _, detail := range status.Convert(failure).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				proved = info.Domain == "agentvmm.afk.surf" && info.Metadata["admission"] == "not_accepted"
			}
		}
		if !proved {
			t.Fatalf("stale admission omitted proof: %v", failure)
		}
	}

	stale := installation
	stale.CurrentBootID = request.ExpectedBootId
	stale.CurrentContainerInstance = request.ExpectedContainerInstanceId
	postCommit := controller.refreshApplianceServiceImports(ctx, stale)
	if status.Code(postCommit) != codes.Aborted {
		t.Fatalf("post-commit projection=%v", postCommit)
	}
	for _, detail := range status.Convert(postCommit).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			if info.Reason != "APPLIANCE_SERVICE_SOURCE_CHANGED" || info.Metadata["admission"] == "not_accepted" {
				t.Fatalf("post-commit error claimed non-admission: %v", postCommit)
			}
		}
	}
	for _, mutate := range []func(*appliancev1.CreateApplianceServiceImportRequest){func(r *appliancev1.CreateApplianceServiceImportRequest) { r.InstallationId = "neighbor" }, func(r *appliancev1.CreateApplianceServiceImportRequest) { r.VirtualServiceName = "neighbor" }} {
		changed := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
		mutate(changed)
		_, err := callAsOperator(t, func(ctx context.Context) (*servicev1.ServiceImport, error) {
			return server.CreateServiceImport(ctx, changed)
		})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("changed owner/payload=%v", err)
		}
	}
}

func TestApplianceServiceImportFailureStaysLocalAndRetries(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	controller.egress = egress.New()
	ctx := context.Background()
	installation.DesiredEnabled, installation.State = true, 3
	installation.CurrentBootID, installation.CurrentContainerInstance = "boot", "instance"
	var err error
	installation, err = controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", OwnerKind: store.OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"bad", "healthy"} {
		exported, err := controller.store.CreateServiceExport(ctx, id, &servicev1.ServiceExport{ExportId: id, EnvironmentId: "service", ContainerId: id, ContainerGenerationId: "generation", ContainerInstanceId: "target", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
		if err != nil {
			t.Fatal(err)
		}
		request := &appliancev1.CreateApplianceServiceImportRequest{RequestId: id, InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: "boot", ExpectedContainerInstanceId: "instance", DestinationExportId: id, ExpectedExportRevision: exported.Revision, VirtualServiceName: id}
		value := &servicev1.ServiceImport{ImportId: id, SourceInstallationId: installation.ID, SourceContainerId: systemContainerID(), DestinationDeviceId: "device", DestinationExportId: id, DestinationPort: 3000, VirtualServiceName: id}
		expires := timestamppb.New(time.Now().Add(2 * time.Hour))
		route := &servicev1.AccessRoute{RouteId: id, RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires, SourceInstallationId: installation.ID, SourceBootId: "boot", SourceContainerInstanceId: "instance", PolicyRevision: 1, DestinationExportId: id}
		lease := &servicev1.RouteLease{LeaseId: id, RouteId: id, RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: &trustv1.RouteCapability{CapabilityId: id, RouteGeneration: 1, ExpiresAt: expires}}
		if _, err := controller.store.CreateApplianceServiceImport(ctx, request, systemNamespaceID(installation.ID), value, route, lease); err != nil {
			t.Fatal(err)
		}
	}
	guest := &recoveryServiceGuest{sourceInstance: "instance", targetInstance: "target", containers: map[string]*vmmdv1.Container{"bad": {Id: "bad", GenerationId: "generation", InstanceId: "target", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}, "healthy": {Id: "healthy", GenerationId: "generation", InstanceId: "target", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}, containerErrors: map[string]codes.Code{"bad": codes.FailedPrecondition}}
	client := serviceRecoveryClient(t, guest)
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	imports, err := controller.store.ListApplianceServiceImports(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range imports {
		if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, value); err != nil {
			t.Fatal("local failure stopped healthy projection", err)
		}
	}
	for _, value := range imports {
		record, err := controller.store.ServiceRouteForImport(ctx, value.ImportId)
		if err != nil {
			t.Fatal(err)
		}
		want := servicev1.RouteState_ROUTE_STATE_ACTIVE
		if value.ImportId == "bad" {
			want = servicev1.RouteState_ROUTE_STATE_PREPARING
		}
		if record.Route.State != want {
			t.Fatalf("%s route=%v want=%v", value.ImportId, record.Route.State, want)
		}
	}
	if guest.applied != 1 {
		t.Fatalf("healthy import was not published: %d", guest.applied)
	}
	firstCleanup := guest.deletedRequestIDs[len(guest.deletedRequestIDs)-1]
	guest.containerErrors["bad"] = codes.OK
	for _, value := range imports {
		if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, value); err != nil {
			t.Fatal(err)
		}
	}
	repaired, err := controller.store.ServiceRouteForImport(ctx, "bad")
	if err != nil || repaired.Route.State != servicev1.RouteState_ROUTE_STATE_ACTIVE || guest.applied != 3 {
		t.Fatal("refresh did not retry bad import", repaired, guest.applied, err)
	}
	guest.containerErrors["bad"] = codes.FailedPrecondition
	if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, imports[0]); err != nil {
		t.Fatal("second local failure escaped its import", err)
	}
	secondCleanup := guest.deletedRequestIDs[len(guest.deletedRequestIDs)-1]
	if secondCleanup == firstCleanup {
		t.Fatal("recovered route reused an old successful cleanup identity")
	}
	failedAgain, err := controller.store.ServiceRouteForImport(ctx, "bad")
	if err != nil || failedAgain.Route.GetState() != servicev1.RouteState_ROUTE_STATE_PREPARING || failedAgain.Route.GetRevision() <= repaired.Route.GetRevision() {
		t.Fatal("second failure did not retire the recovered route", failedAgain, err)
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Unknown} {
		guest.containerErrors["bad"] = code
		if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, imports[0]); err != nil {
			t.Fatalf("per-import %s failure escaped its route: %v", code, err)
		}
		record, err := controller.store.ServiceRouteForImport(ctx, "bad")
		if err != nil || record.Route.GetState() != servicev1.RouteState_ROUTE_STATE_PREPARING {
			t.Fatalf("failed route remained active: %v %v", record, err)
		}
		if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, imports[1]); err != nil {
			t.Fatal("healthy import blocked", err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := controller.refreshApplianceServiceImportLocked(cancelled, client, installation, grant, imports[0]); !errors.Is(err, context.Canceled) {
		t.Fatal("parent cancellation was hidden", err)
	}
	controller.store.Close()
	if err := controller.refreshApplianceServiceImportLocked(ctx, client, installation, grant, imports[0]); err == nil || status.Code(err) != codes.Unknown {
		t.Fatal("Store failure was hidden", err)
	}
}
