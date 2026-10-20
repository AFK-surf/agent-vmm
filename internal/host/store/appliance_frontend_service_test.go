package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"google.golang.org/protobuf/proto"
)

func frontendServiceFixture(t *testing.T) (*Store, ApplianceInstallation, *appliancev1.CreateApplianceFrontendServiceImportRequest, *servicev1.ServiceImport, *servicev1.AccessRoute, *servicev1.RouteLease) {
	t.Helper()
	database, installation, _, _, route, lease := applianceServiceFixture(t)
	ctx := context.Background()
	if _, err := database.db.ExecContext(ctx, `INSERT INTO appliance_frontends(installation_id,name,transport,exposure,max_frame_bytes,max_total_bytes,deadline_seconds,idle_seconds,max_concurrent_streams) VALUES(?,'webhook',2,3,1024,4096,10,5,2)`, installation.ID); err != nil {
		t.Fatal(err)
	}
	request := &appliancev1.CreateApplianceFrontendServiceImportRequest{RequestId: "callback", InstallationId: installation.ID, ExpectedRevision: installation.Revision, ExpectedGrantRevision: 1, ExpectedBootId: installation.CurrentBootID, ExpectedContainerInstanceId: installation.CurrentContainerInstance, SourceEnvironmentId: "gitea", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", FrontendName: "webhook", VirtualServiceName: "agentd-webhook"}
	value := &servicev1.ServiceImport{ImportId: "callback", SourceEnvironmentId: "gitea", SourceContainerId: "gitea", SourceContainerInstanceId: "gitea-instance", DestinationDeviceId: "device", DestinationFrontend: &servicev1.ApplianceFrontendDestination{InstallationId: installation.ID, Name: "webhook"}, DestinationPort: 80, VirtualServiceName: "agentd-webhook"}
	route.DestinationExportId = ""
	route.SourceInstallationId = ""
	route.SourceBootId = ""
	route.SourceEnvironmentId = "gitea"
	route.SourceContainerInstanceId = "gitea-instance"
	route.DestinationInstallationId = installation.ID
	route.DestinationBootId = installation.CurrentBootID
	route.DestinationContainerInstanceId = installation.CurrentContainerInstance
	route.DestinationGrantRevision = 1
	return database, installation, request, value, route, lease
}

func TestApplianceFrontendIntentExactAuthorityAndReplay(t *testing.T) {
	database, installation, request, value, route, lease := frontendServiceFixture(t)
	ctx := context.Background()
	for _, mutate := range []func(*appliancev1.CreateApplianceFrontendServiceImportRequest){func(r *appliancev1.CreateApplianceFrontendServiceImportRequest) { r.ExpectedRevision++ }, func(r *appliancev1.CreateApplianceFrontendServiceImportRequest) { r.ExpectedGrantRevision++ }, func(r *appliancev1.CreateApplianceFrontendServiceImportRequest) { r.ExpectedBootId = "other" }, func(r *appliancev1.CreateApplianceFrontendServiceImportRequest) {
		r.ExpectedContainerInstanceId = "other"
	}, func(r *appliancev1.CreateApplianceFrontendServiceImportRequest) { r.FrontendName = "management" }} {
		invalid := proto.Clone(request).(*appliancev1.CreateApplianceFrontendServiceImportRequest)
		mutate(invalid)
		if _, err := database.CreateApplianceFrontendServiceImport(ctx, invalid, value, route, lease); err == nil {
			t.Fatal("invalid authority accepted")
		}
	}
	created, err := database.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease)
	if err != nil {
		t.Fatal(err)
	}
	if created.GetDestinationFrontend().GetInstallationId() != installation.ID || created.DestinationExportId != "" {
		t.Fatal(created)
	}
	record, err := database.GetServiceRoute(ctx, route.RouteId, lease.LeaseId, route.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if record.Export != nil || record.Route.DestinationContainerInstanceId != installation.CurrentContainerInstance {
		t.Fatal(record)
	}
	local, err := database.ListActiveServiceProjections(ctx, time.Now())
	if err != nil || len(local) != 0 {
		t.Fatalf("callback in export projections: %v %v", local, err)
	}
	remote, err := database.ListActiveRemoteServiceProjections(ctx, "other-device", time.Now())
	if err != nil || len(remote) != 0 {
		t.Fatalf("callback in remote projections: %v %v", remote, err)
	}
	ids, err := database.WithdrawApplianceServiceImports(ctx, installation.ID)
	if err != nil || len(ids) != 1 {
		t.Fatalf("%v %v", ids, err)
	}
	replay, err := database.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease)
	if err != nil || replay.Revoked {
		t.Fatalf("stop lost intent: %v %v", replay, err)
	}
	withdrawn, err := database.ServiceRouteForImport(ctx, value.ImportId)
	if err != nil {
		t.Fatal(err)
	}
	if withdrawn.Route.State != servicev1.RouteState_ROUTE_STATE_REVOKED {
		t.Fatal("stop retained active route")
	}
	installation.CurrentBootID = "replacement-boot"
	installation.CurrentContainerInstance = "replacement-controller"
	installation.Revision++
	if _, err := database.db.ExecContext(ctx, `UPDATE appliance_installations SET revision=?,current_boot_id=?,current_container_instance=? WHERE id=?`, installation.Revision, installation.CurrentBootID, installation.CurrentContainerInstance, installation.ID); err != nil {
		t.Fatal(err)
	}
	route.DestinationBootId = installation.CurrentBootID
	route.DestinationContainerInstanceId = installation.CurrentContainerInstance
	route.Revision = withdrawn.Route.Revision + 1
	route.Generation = withdrawn.Route.Generation + 1
	lease.RouteRevision = route.Revision
	lease.Generation = route.Generation
	if err := database.RenewApplianceFrontendServiceRoute(ctx, installation, 1, withdrawn.Route.Revision, route, lease); err != nil {
		t.Fatal(err)
	}
	revoke := &appliancev1.RevokeApplianceServiceImportRequest{RequestId: "revoke", InstallationId: installation.ID, ExpectedInstallationRevision: installation.Revision, ImportId: value.ImportId, ExpectedRevision: created.Revision}
	if err := database.RevokeApplianceServiceImport(ctx, revoke); err != nil {
		t.Fatal(err)
	}
	if err := database.RevokeApplianceServiceImport(ctx, revoke); err != nil {
		t.Fatal(err)
	}
	if err := database.RenewApplianceFrontendServiceRoute(ctx, installation, 1, route.Revision, route, lease); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("revoked restored: %v", err)
	}
}

func TestApplianceFrontendSourceAndRemovedGrantCannotRevive(t *testing.T) {
	for _, boundary := range []string{"source-instance", "frontend-removed", "frontend-transport", "frontend-outbound", "grant-revision"} {
		t.Run(boundary, func(t *testing.T) {
			database, installation, request, value, route, lease := frontendServiceFixture(t)
			ctx := context.Background()
			if _, err := database.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease); err != nil {
				t.Fatal(err)
			}
			switch boundary {
			case "source-instance":
				ids, err := database.ProjectServiceContainer(ctx, "gitea", "gitea", "replacement-generation", "replacement")
				if err != nil || len(ids) != 1 {
					t.Fatalf("%v %v", ids, err)
				}
			case "frontend-removed", "frontend-transport", "frontend-outbound":
				statement := `DELETE FROM appliance_frontends WHERE installation_id=?`
				if boundary == "frontend-transport" {
					statement = `UPDATE appliance_frontends SET transport=1,exposure=1 WHERE installation_id=?`
				}
				if boundary == "frontend-outbound" {
					statement = `UPDATE appliance_frontends SET exposure=2 WHERE installation_id=?`
				}
				if _, err := database.db.ExecContext(ctx, statement, installation.ID); err != nil {
					t.Fatal(err)
				}
				tx, err := database.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := revokeMissingApplianceFrontends(ctx, tx, installation.ID, time.Now().UnixNano()); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if _, err := database.db.ExecContext(ctx, `INSERT OR REPLACE INTO appliance_frontends(installation_id,name,transport,exposure,max_frame_bytes,max_total_bytes,deadline_seconds,idle_seconds,max_concurrent_streams) VALUES(?,'webhook',2,3,1024,4096,10,5,2)`, installation.ID); err != nil {
					t.Fatal(err)
				}
			case "grant-revision":
				if _, err := database.db.ExecContext(ctx, `UPDATE appliance_grants SET revision=2 WHERE installation_id=?`, installation.ID); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.RenewApplianceFrontendServiceRoute(ctx, installation, 1, route.Revision, route, lease); err == nil {
				t.Fatal("invalidated source/grant revived")
			}
		})
	}
}

func TestApplianceRemoveDeletesRevokedFrontendReferencesAndResumesOriginalReceipt(t *testing.T) {
	database, installation, request, value, route, lease := frontendServiceFixture(t)
	ctx := context.Background()
	if _, err := database.CreateApplianceFrontendServiceImport(ctx, request, value, route, lease); err != nil {
		t.Fatal(err)
	}
	neighbor := proto.Clone(value).(*servicev1.ServiceImport)
	neighbor.ImportId, neighbor.VirtualServiceName = "neighbor", "neighbor"
	neighbor.DestinationFrontend = nil
	neighbor.DestinationExportId, neighbor.DestinationPort = "export", 3000
	neighborRoute := proto.Clone(route).(*servicev1.AccessRoute)
	neighborRoute.RouteId, neighborRoute.DestinationExportId = "neighbor-route", "export"
	neighborRoute.DestinationInstallationId, neighborRoute.DestinationBootId = "", ""
	neighborRoute.DestinationContainerInstanceId, neighborRoute.DestinationGrantRevision = "", 0
	neighborLease := proto.Clone(lease).(*servicev1.RouteLease)
	neighborLease.LeaseId, neighborLease.RouteId = "neighbor-lease", neighborRoute.RouteId
	neighbor, err := database.CreateServiceImport(ctx, "neighbor", neighbor, neighborRoute, neighborLease)
	if err != nil {
		t.Fatal(err)
	}
	installation.DesiredEnabled = false
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED)
	if _, err := database.BeginApplianceStateMutation(ctx, installation, installation.Revision, ApplianceOperation{
		ID: "revoke", InstallationID: installation.ID, RequestID: "revoke",
		Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE), State: 2, Phase: 5,
	}); err != nil {
		t.Fatal(err)
	}
	installation, err = database.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := database.BeginApplianceRemove(ctx, ApplianceRemove{
		InstallationID: installation.ID, OperationID: "remove", ExpectedRevision: installation.Revision,
		ExpectedState: installation.State, PendingState: int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING),
		ExpectedPhase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
		Operation: ApplianceOperation{ID: "remove", InstallationID: installation.ID, RequestID: "original-remove",
			Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REMOVE), State: 1, Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED),
			ExpectedRevision: installation.Revision},
	})
	if err != nil {
		t.Fatal(err)
	}
	remove := ApplianceRemove{InstallationID: pending.ID, RegistrationID: pending.RegistrationID,
		OperationID: "remove", CommitRevision: pending.Revision, Receipt: []byte("removed")}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER fail_remove_receipt BEFORE UPDATE ON appliance_operations
		WHEN NEW.id='remove' AND NEW.state=2 BEGIN SELECT RAISE(ABORT,'fixture receipt failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CommitApplianceRemove(ctx, remove); err == nil || !strings.Contains(err.Error(), "fixture receipt failure") {
		t.Fatalf("remove did not reach atomic receipt completion: %v", err)
	}
	if _, err := database.TransitionApplianceOperation(ctx, remove.OperationID, ApplianceOperationTransition{
		ExpectedState: 1, ExpectedPhase: 4, State: 1, Phase: 4, Stage: "pending-destroy",
		ErrorReason: "APPLIANCE_REMOVE_PENDING", ErrorMessage: "fixture receipt failure",
	}); err != nil {
		t.Fatal(err)
	}
	retained, err := database.GetServiceImport(ctx, value.ImportId)
	if err != nil || !retained.Revoked {
		t.Fatalf("failed remove lost revoked callback: %v %v", retained, err)
	}
	if _, err := database.ServiceRouteForImport(ctx, value.ImportId); err != nil {
		t.Fatalf("failed remove lost callback route/lease: %v", err)
	}
	var path string
	if err := database.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	operation, err := database.ApplianceOperationByRequest(ctx, "original-remove")
	if err != nil || operation.ID != remove.OperationID || operation.State != 1 || operation.Phase != 4 {
		t.Fatalf("original removal did not remain pending: %+v %v", operation, err)
	}
	if _, err := database.db.ExecContext(ctx, `DROP TRIGGER fail_remove_receipt`); err != nil {
		t.Fatal(err)
	}
	committed, err := database.CommitApplianceRemove(ctx, remove)
	if err != nil || committed.ID != operation.ID || committed.State != 2 || committed.Phase != 5 || committed.ErrorReason != "" || committed.ErrorMessage != "" {
		t.Fatalf("original remove could not resume: %+v %v", committed, err)
	}
	if _, err := database.GetApplianceInstallation(ctx, installation.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removed installation survived: %v", err)
	}
	if _, err := database.GetRemoteRegistration(ctx, installation.RegistrationID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removed registration survived: %v", err)
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM service_imports WHERE import_id='callback'`,
		`SELECT COUNT(*) FROM access_routes WHERE route_id='route'`,
		`SELECT COUNT(*) FROM route_leases WHERE lease_id='lease'`,
	} {
		var count int
		if err := database.db.QueryRowContext(ctx, query).Scan(&count); err != nil || count != 0 {
			t.Fatalf("callback reference survived remove: count=%d error=%v", count, err)
		}
	}
	retained, err = database.GetServiceImport(ctx, neighbor.ImportId)
	if err != nil || !proto.Equal(retained, neighbor) {
		t.Fatalf("unrelated service approval changed: %v %v", retained, err)
	}
	if _, err := database.ServiceRouteForImport(ctx, neighbor.ImportId); err != nil {
		t.Fatalf("unrelated service route/lease changed: %v", err)
	}
	if _, err := database.GetEnvironment(ctx, "gitea"); err != nil {
		t.Fatalf("external service environment removed: %v", err)
	}
	if _, err := database.GetServiceExport(ctx, "export"); err != nil {
		t.Fatalf("external service export removed: %v", err)
	}
}
