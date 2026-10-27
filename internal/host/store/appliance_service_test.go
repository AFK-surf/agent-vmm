package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func applianceServiceFixture(t *testing.T) (*Store, ApplianceInstallation, *appliancev1.CreateApplianceServiceImportRequest, *servicev1.ServiceImport, *servicev1.AccessRoute, *servicev1.RouteLease) {
	t.Helper()
	database, _ := applianceTrustStore(t)
	ctx := context.Background()
	installation, err := database.GetApplianceInstallation(ctx, "installation")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "gitea", NamespaceID: "gitea-ns", DisplayName: "gitea", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	exported, err := database.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "gitea", ContainerId: "gitea", ContainerInstanceId: "gitea-instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	request := &appliancev1.CreateApplianceServiceImportRequest{RequestId: "approve", InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: installation.CurrentBootID, ExpectedContainerInstanceId: installation.CurrentContainerInstance, DestinationExportId: exported.ExportId, ExpectedExportRevision: exported.Revision, VirtualServiceName: "gitea"}
	value := &servicev1.ServiceImport{ImportId: "import", SourceInstallationId: installation.ID, SourceContainerId: "payload", DestinationDeviceId: "device", DestinationExportId: exported.ExportId, DestinationPort: 3000, VirtualServiceName: "gitea"}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	route := &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceDeviceId: "device", DestinationDeviceId: "device", DestinationExportId: exported.ExportId, State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 7, ExpiresAt: expires, SourceInstallationId: installation.ID, SourceBootId: installation.CurrentBootID, SourceContainerInstanceId: installation.CurrentContainerInstance, PolicyRevision: 1}
	lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: route.RouteId, RouteRevision: route.Revision, Generation: route.Generation, ExpiresAt: expires, Capability: &trustv1.RouteCapability{CapabilityId: "capability", RouteGeneration: route.Generation, PolicyRevision: 1, ExpiresAt: expires}}
	return database, installation, request, value, route, lease
}

func TestApplianceServiceIntentPersistsAcrossStopAndExactInstanceReprojection(t *testing.T) {
	database, installation, request, value, route, lease := applianceServiceFixture(t)
	ctx := context.Background()
	for _, mutate := range []func(*appliancev1.CreateApplianceServiceImportRequest){func(r *appliancev1.CreateApplianceServiceImportRequest) { r.ExpectedGrantRevision++ }, func(r *appliancev1.CreateApplianceServiceImportRequest) { r.ExpectedBootId = "neighbor" }, func(r *appliancev1.CreateApplianceServiceImportRequest) { r.ExpectedExportRevision++ }} {
		changed := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
		mutate(changed)
		if _, err := database.CreateApplianceServiceImport(ctx, changed, "system.namespace", value, route, lease); !errors.Is(err, ErrInstallationRevisionConflict) {
			t.Fatalf("stale source/export=%v", err)
		}
	}
	created, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease)
	if err != nil {
		t.Fatal(err)
	}
	projections, err := database.ListActiveServiceProjections(ctx, time.Now())
	if err != nil || len(projections) != 1 || projections[0].SourceNamespace != "system.namespace" || projections[0].Import.SourceContainerInstanceId != installation.CurrentContainerInstance || created.SourceEnvironmentId != "" {
		t.Fatalf("projection=%+v err=%v", projections, err)
	}
	installation.DesiredEnabled = false
	installation.State = 1
	if _, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{ID: "stop", RequestID: "stop", Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STOP), State: 2, Phase: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.WithdrawApplianceServiceImports(ctx, installation.ID); err != nil {
		t.Fatal(err)
	}
	preserved, err := database.GetServiceImport(ctx, created.ImportId)
	if err != nil || preserved.Revoked {
		t.Fatalf("stop lost intent: %v %v", preserved, err)
	}
	projections, err = database.ListActiveServiceProjections(ctx, time.Now())
	if err != nil || len(projections) != 0 {
		t.Fatalf("stopped source still projected=%v %v", projections, err)
	}
	installation, err = database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = true
	installation.State = 3
	installation.CurrentBootID = "new-boot"
	installation.CurrentContainerInstance = "new-instance"
	installation, err = database.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	record, err := database.ServiceRouteForImport(ctx, created.ImportId)
	if err != nil {
		t.Fatal(err)
	}
	renewed := proto.Clone(record.Route).(*servicev1.AccessRoute)
	renewed.Revision++
	renewed.Generation++
	renewed.SourceBootId = installation.CurrentBootID
	renewed.SourceContainerInstanceId = installation.CurrentContainerInstance
	nextLease := proto.Clone(record.Lease).(*servicev1.RouteLease)
	nextLease.RouteRevision = renewed.Revision
	nextLease.Generation = renewed.Generation
	if err := database.RenewApplianceServiceRoute(ctx, installation, 1, record.Route.Revision, renewed, nextLease); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetServiceRoute(ctx, route.RouteId, lease.LeaseId, route.Generation); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("old route generation remained usable=%v", err)
	}
	projections, err = database.ListActiveServiceProjections(ctx, time.Now())
	if err != nil || len(projections) != 1 || projections[0].Import.SourceContainerInstanceId != "new-instance" || projections[0].Import.DestinationExportId != created.DestinationExportId {
		t.Fatalf("restored projection=%v %v", projections, err)
	}
	revoke := &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: created.ImportId, ExpectedRevision: created.Revision}
	for range 2 {
		if err := database.RevokeApplianceServiceImport(ctx, revoke); err != nil {
			t.Fatal(err)
		}
	}
	record, err = database.ServiceRouteForImport(ctx, created.ImportId)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RenewApplianceServiceRoute(ctx, installation, 1, record.Route.Revision, renewed, nextLease); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("revoked intent reactivated=%v", err)
	}
	replay, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease)
	if err != nil || !replay.Revoked {
		t.Fatalf("old request revived intent=%v %v", replay, err)
	}
	changed := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
	changed.ExpectedBootId = installation.CurrentBootID
	if _, err := database.CreateApplianceServiceImport(ctx, changed, "system.namespace", value, route, lease); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("old approval changed source fence=%v", err)
	}
}

func TestApplianceServiceIntentTerminationIsAtomicWithInstallationFence(t *testing.T) {
	for _, kind := range []appliancev1.ApplianceOperationKind{appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE} {
		t.Run(kind.String(), func(t *testing.T) {
			database, installation, request, value, route, lease := applianceServiceFixture(t)
			ctx := context.Background()
			if _, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease); err != nil {
				t.Fatal(err)
			}
			operation := ApplianceOperation{ID: "terminate", RequestID: "terminate", InstallationID: installation.ID, Kind: int32(kind), State: 1, Phase: 2}
			terminate := func(revision uint64) error {
				if kind == appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE {
					_, err := database.BeginApplianceRemove(ctx, ApplianceRemove{InstallationID: installation.ID, OperationID: operation.ID, ExpectedRevision: revision, ExpectedState: installation.State, PendingState: 4, ExpectedPhase: 2, Operation: operation})
					return err
				}
				stopped := installation
				stopped.DesiredEnabled = false
				stopped.State = 1
				_, err := database.BeginApplianceStateMutation(ctx, stopped, revision, operation)
				return err
			}
			if err := terminate(installation.Revision + 1); !errors.Is(err, ErrInstallationRevisionConflict) {
				t.Fatal(err)
			}
			prior, err := database.GetServiceImport(ctx, value.ImportId)
			if err != nil || prior.Revoked {
				t.Fatalf("failed CAS revoked intent: %v %v", prior, err)
			}
			before, err := database.ServiceRouteForImport(ctx, value.ImportId)
			if err != nil || before.Route.GetState() != servicev1.RouteState_ROUTE_STATE_ACTIVE || before.Route.GetGeneration() != route.Generation {
				t.Fatal("failed installation CAS changed route", before, err)
			}
			if err := terminate(installation.Revision); err != nil {
				t.Fatal(err)
			}
			after, err := database.ServiceRouteForImport(ctx, value.ImportId)
			if err != nil || after.Route.GetState() != servicev1.RouteState_ROUTE_STATE_REVOKED || after.Route.GetGeneration() != route.Generation+1 {
				t.Fatal("installation termination did not fence route atomically", after, err)
			}
			replay, err := database.ReplayApplianceServiceImport(ctx, request)
			if err != nil || !replay.Revoked {
				t.Fatalf("termination receipt=%v %v", replay, err)
			}
			projections, err := database.ListActiveServiceProjections(ctx, time.Now())
			if err != nil || len(projections) != 0 {
				t.Fatalf("terminated source projection=%v %v", projections, err)
			}
		})
	}
}

func TestApplianceServiceRenewalCannotReviveConcurrentRevocation(t *testing.T) {
	database, installation, request, value, route, lease := applianceServiceFixture(t)
	ctx := context.Background()
	if _, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease); err != nil {
		t.Fatal(err)
	}
	next := proto.Clone(route).(*servicev1.AccessRoute)
	next.Revision++
	next.Generation++
	nextLease := proto.Clone(lease).(*servicev1.RouteLease)
	nextLease.RouteRevision = next.Revision
	nextLease.Generation = next.Generation
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- database.RenewApplianceServiceRoute(ctx, installation, 1, route.Revision, next, nextLease)
	}()
	go func() {
		<-start
		results <- database.RevokeApplianceServiceImport(ctx, &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: value.ImportId, ExpectedRevision: 1})
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil && !errors.Is(err, ErrTrustRevisionConflict) {
			t.Fatal(err)
		}
	}
	intent, err := database.GetServiceImport(ctx, value.ImportId)
	if err != nil || !intent.Revoked {
		t.Fatalf("intent=%v %v", intent, err)
	}
	record, err := database.ServiceRouteForImport(ctx, value.ImportId)
	if err != nil || record.Route.State != servicev1.RouteState_ROUTE_STATE_REVOKED {
		t.Fatalf("route=%v %v", record, err)
	}
	projections, err := database.ListActiveServiceProjections(ctx, time.Now())
	if err != nil || len(projections) != 0 {
		t.Fatalf("revoked source projection=%v %v", projections, err)
	}
}

func TestApplianceServiceDestinationRevocationReleasesAliasWithoutRedirectingIntent(t *testing.T) {
	database, _, request, value, route, lease := applianceServiceFixture(t)
	ctx := context.Background()
	if _, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease); err != nil {
		t.Fatal(err)
	}
	if err := database.RevokeServiceExport(ctx, "gitea", value.DestinationExportId, 1); err != nil {
		t.Fatal(err)
	}
	replay, err := database.ReplayApplianceServiceImport(ctx, request)
	if err != nil || !replay.Revoked || replay.DestinationExportId != value.DestinationExportId {
		t.Fatalf("old exact approval=%v %v", replay, err)
	}
	newExport, err := database.CreateServiceExport(ctx, "new-export", &servicev1.ServiceExport{ExportId: "new-export", EnvironmentId: "gitea", ContainerId: "gitea", ContainerInstanceId: "new-gitea-instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	newRequest := proto.Clone(request).(*appliancev1.CreateApplianceServiceImportRequest)
	newRequest.RequestId = "new-approve"
	newRequest.DestinationExportId = newExport.ExportId
	newValue := proto.Clone(value).(*servicev1.ServiceImport)
	newValue.ImportId = "new-import"
	newValue.DestinationExportId = newExport.ExportId
	newRoute := proto.Clone(route).(*servicev1.AccessRoute)
	newRoute.RouteId = "new-route"
	newRoute.DestinationExportId = newExport.ExportId
	newLease := proto.Clone(lease).(*servicev1.RouteLease)
	newLease.LeaseId = "new-lease"
	newLease.RouteId = newRoute.RouteId
	created, err := database.CreateApplianceServiceImport(ctx, newRequest, "system.namespace", newValue, newRoute, newLease)
	if err != nil || created.Revoked || created.VirtualServiceName != value.VirtualServiceName {
		t.Fatalf("explicit new approval=%v %v", created, err)
	}
	replay, err = database.ReplayApplianceServiceImport(ctx, request)
	if err != nil || !replay.Revoked || replay.DestinationExportId == created.DestinationExportId {
		t.Fatalf("old approval redirected=%v %v", replay, err)
	}
}

func TestInstallationServiceApprovalProjectsOnlyToItsPrivateRegistration(t *testing.T) {
	database, installation, request, value, route, lease := applianceServiceFixture(t)
	ctx := context.Background()
	if _, err := database.CreateApplianceServiceImport(ctx, request, "system.namespace", value, route, lease); err != nil {
		t.Fatal(err)
	}
	approved, err := database.ListApprovedServiceExports(ctx, installation.RegistrationID)
	if err != nil || len(approved) != 1 || approved[0].GetExportId() != "export" {
		t.Fatalf("approved=%v err=%v", approved, err)
	}
	for _, registration := range []string{"neighbor-registration", ""} {
		ok, err := database.ServiceExportApproved(ctx, registration, "export")
		if err != nil || ok {
			t.Fatalf("neighbor approval=%t err=%v", ok, err)
		}
	}
	if _, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "external", DisplayName: "external", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1, OwnerKind: RemoteRegistrationExternal}); err != nil {
		t.Fatal(err)
	}
	if ok, err := database.ServiceExportApproved(ctx, "external", "export"); err != nil || ok {
		t.Fatalf("external registration inherited private approval: %t %v", ok, err)
	}
	if _, err := database.db.ExecContext(ctx, `UPDATE remote_registrations SET owner_kind=? WHERE id=?`, RemoteRegistrationExternal, installation.RegistrationID); err == nil {
		t.Fatal("private installation owner was rebound")
	}
	if err := database.RevokeApplianceServiceImport(ctx, &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke-approval", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: value.ImportId, ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if ok, err := database.ServiceExportApproved(ctx, installation.RegistrationID, "export"); err != nil || ok {
		t.Fatalf("revoked approval persisted: %t %v", ok, err)
	}
}
