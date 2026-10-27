//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestApplianceServiceImportFailureStaysLocalAndRetries(t *testing.T) {
	controller, installation := updateDrainFixture(t)

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
	guest := &serviceProjectionRecoveryGuest{sourceInstance: "instance", targetInstance: "target", containers: map[string]*vmmdv1.Container{"bad": {Id: "bad", GenerationId: "generation", InstanceId: "target", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}, "healthy": {Id: "healthy", GenerationId: "generation", InstanceId: "target", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING}}, containerErrors: map[string]codes.Code{"bad": codes.FailedPrecondition}}
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
