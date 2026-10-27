package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServiceRoutePersistenceAndRevocationFences(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, environment := range []Environment{
		{ID: "source", NamespaceID: "source-ns", DisplayName: "Source", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal},
		{ID: "destination", NamespaceID: "destination-ns", DisplayName: "Destination", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal},
	} {
		if _, err := database.CreateEnvironment(ctx, environment); err != nil {
			t.Fatal(err)
		}
	}
	exported, err := database.CreateServiceExport(ctx, "export-request", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: "destination", ContainerId: "server", ContainerInstanceId: "server-instance", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	capability := &trustv1.RouteCapability{CapabilityId: "capability", SourceDeviceId: "source-device", DestinationDeviceId: "destination-device", DestinationExportId: "export", RouteGeneration: 7, PolicyRevision: 3, ExpiresAt: expires, Audience: "agent-vmm/service-tunnel/1", Budget: &trustv1.RouteBudget{ConnectionLimit: 1, ByteLimit: 1024, ConcurrencyLimit: 1}}
	serviceImport := &servicev1.ServiceImport{ImportId: "import", SourceEnvironmentId: "source", SourceContainerId: "client", SourceContainerInstanceId: "client-instance", DestinationDeviceId: "destination-device", DestinationExportId: "export", VirtualServiceName: "database"}
	route := &servicev1.AccessRoute{RouteId: "route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, SourceDeviceId: "source-device", DestinationDeviceId: "destination-device", DestinationExportId: "export", State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 7, ExpiresAt: expires, SourceEnvironmentId: "source", SourceContainerInstanceId: "client-instance", PolicyRevision: 3}
	lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: "route", RouteRevision: 1, Generation: 7, ExpiresAt: expires, Capability: capability}
	created, err := database.CreateServiceImport(ctx, "import-request", serviceImport, route, lease)
	if err != nil || created.GetImportId() != "import" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	record, err := database.GetServiceRoute(ctx, "route", "lease", 7)
	if err != nil || record.Import.GetSourceContainerId() != "client" || record.Export.GetContainerInstanceId() != "server-instance" || record.Capability.GetCapabilityId() != "capability" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if _, err := database.CreateServiceExport(ctx, "export-request", &servicev1.ServiceExport{ExportId: "different", EnvironmentId: "destination", ContainerId: "server", ContainerInstanceId: "new-instance", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
		t.Fatalf("export request idempotency failed: %v", err)
	}
	if err := database.RevokeServiceExport(ctx, "destination", "export", exported.GetRevision()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetServiceRoute(ctx, "route", "lease", 7); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("revoked route remained usable at old generation: %v", err)
	}
	record, err = database.GetServiceRoute(ctx, "route", "lease", 8)
	if err != nil || record.Route.GetState() != servicev1.RouteState_ROUTE_STATE_REVOKED || record.Route.GetGeneration() != 8 || record.Lease.GetGeneration() != 7 || !record.Export.GetRevoked() {
		t.Fatalf("fenced record=%+v err=%v", record, err)
	}
	var state servicev1.RouteState
	var generation uint64
	if err := database.db.QueryRowContext(ctx, `SELECT state, generation FROM access_routes WHERE route_id='route'`).Scan(&state, &generation); err != nil || state != servicev1.RouteState_ROUTE_STATE_REVOKED || generation != 8 {
		t.Fatalf("state=%s generation=%d err=%v", state, generation, err)
	}
	if !record.Import.GetRevoked() || record.Import.GetRevision() != created.GetRevision()+1 {
		t.Fatalf("export revoke retained import approval: %v", record.Import)
	}
	if err := database.RevokeServiceImport(ctx, "source", created.GetImportId(), created.GetRevision()); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("stale import revision after export revoke=%v", err)
	}
	replay, err := database.CreateServiceImport(ctx, "import-request", serviceImport, route, lease)
	if err != nil || !replay.GetRevoked() || replay.GetImportId() != created.GetImportId() {
		t.Fatalf("revoked request replay=%v err=%v", replay, err)
	}
	nextExport := proto.Clone(exported).(*servicev1.ServiceExport)
	nextExport.ExportId = "next-export"
	nextExport.ContainerInstanceId = "next-server-instance"
	if _, err := database.CreateServiceExport(ctx, "next-export-request", nextExport); err != nil {
		t.Fatal(err)
	}
	nextImport := proto.Clone(serviceImport).(*servicev1.ServiceImport)
	nextImport.ImportId = "next-import"
	nextImport.DestinationExportId = "next-export"
	nextRoute := proto.Clone(route).(*servicev1.AccessRoute)
	nextRoute.RouteId = "next-route"
	nextRoute.DestinationExportId = "next-export"
	nextLease := proto.Clone(lease).(*servicev1.RouteLease)
	nextLease.LeaseId = "next-lease"
	nextLease.RouteId = nextRoute.RouteId
	if _, err := database.CreateServiceImport(ctx, "next-request", nextImport, nextRoute, nextLease); err != nil {
		t.Fatalf("explicit new request could not reuse revoked alias: %v", err)
	}
	nextImport.DestinationExportId = "replacement-export"
	if _, err := database.CreateServiceImport(ctx, "import-request", nextImport, nextRoute, nextLease); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("old request changed target: %v", err)
	}

}

func TestMutationReceiptsAreAgeAndCountBounded(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	old := time.Now().Add(-auditRetention - time.Hour).UnixNano()
	if _, err := database.db.ExecContext(ctx, `INSERT INTO audit(actor,target,method,grant_id,request_id,result_code,reason,occurred_at)
		VALUES ('a','t','old','','',0,'',?)`, old); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < auditMaxRows+5; index++ {
		if err := database.Audit(ctx, "a", "t", "mutation", "", fmt.Sprint(index), 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	var count, oldCount int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(occurred_at < ?),0) FROM audit`, time.Now().Add(-auditRetention).UnixNano()).Scan(&count, &oldCount); err != nil {
		t.Fatal(err)
	}
	if count != auditMaxRows || oldCount != 0 {
		t.Fatalf("receipt rows=%d old=%d", count, oldCount)
	}
	var autoCheckpoint, journalLimit int64
	if err := database.db.QueryRowContext(ctx, "PRAGMA wal_autocheckpoint").Scan(&autoCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(ctx, "PRAGMA journal_size_limit").Scan(&journalLimit); err != nil {
		t.Fatal(err)
	}
	if autoCheckpoint != 1000 || journalLimit != 64<<20 {
		t.Fatalf("wal bounds=%d/%d", autoCheckpoint, journalLimit)
	}
}

func TestOldAuditMaintenanceUsesBoundedBatches(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	old := time.Now().Add(-auditRetention - time.Hour).UnixNano()
	for index := 0; index < auditPruneBatch*3; index++ {
		if _, err := database.db.ExecContext(ctx, `INSERT INTO audit(actor,target,method,grant_id,request_id,result_code,reason,occurred_at)
			VALUES ('a','t','old','','',0,'',?)`, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.maintainStorage(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != auditPruneBatch*2 {
		t.Fatalf("one maintenance pass removed an unbounded set: remaining=%d", remaining)
	}
	if err := database.maintainStorage(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := database.maintainStorage(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("subsequent owner passes did not converge: remaining=%d err=%v", remaining, err)
	}
}

func TestWALConvergesAfterLongReaderReleasesSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if err := database.Audit(ctx, "actor", "target", "seed", "", "seed", 0, ""); err != nil {
		t.Fatal(err)
	}
	reader, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	readTx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var initial int
	if err := readTx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&initial); err != nil {
		readTx.Rollback()
		t.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		if err := database.Audit(ctx, "actor", "target", "mutation", "", fmt.Sprint(index), 0, ""); err != nil {
			readTx.Rollback()
			t.Fatal(err)
		}
	}
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
		readTx.Rollback()
		t.Fatalf("held reader did not retain WAL frames: info=%v err=%v", info, err)
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := database.Audit(ctx, "actor", "target", "post-reader-mutation", "", "post-reader", 0, ""); err != nil {
		t.Fatal(err)
	}
	var busy, logFrames, checkpointed int
	if err := database.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || logFrames != 0 || checkpointed != 0 {
		t.Fatalf("WAL did not fully truncate after reader release: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() > 64<<20 {
		t.Fatalf("WAL exceeded its post-reader bound: info=%v err=%v", info, err)
	}
	var current int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&current); err != nil || current != initial+129 {
		t.Fatalf("checkpoint damaged mutation receipts: count=%d err=%v", current, err)
	}
}

func TestServiceExportsAndImportsFollowReplacementContainerGeneration(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, environment := range []Environment{
		{ID: "source", NamespaceID: "source-ns", DisplayName: "Source", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal},
		{ID: "destination", NamespaceID: "destination-ns", DisplayName: "Destination", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal},
	} {
		if _, err := database.CreateEnvironment(ctx, environment); err != nil {
			t.Fatal(err)
		}
	}
	exported, err := database.CreateServiceExport(ctx, "stale-export-request", &servicev1.ServiceExport{ExportId: "stale-export", EnvironmentId: "destination", ContainerId: "server", ContainerInstanceId: "old-instance", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
	if err != nil {
		t.Fatal(err)
	}
	expires := timestamppb.New(time.Now().Add(time.Hour))
	capability := &trustv1.RouteCapability{CapabilityId: "stale-capability", DestinationExportId: exported.GetExportId(), ExpiresAt: expires}
	if _, err := database.CreateServiceImport(ctx, "stale-import-request",
		&servicev1.ServiceImport{ImportId: "stale-import", SourceEnvironmentId: "source", SourceContainerId: "client", SourceContainerInstanceId: "client-instance", DestinationDeviceId: "destination-device", DestinationExportId: exported.GetExportId(), VirtualServiceName: "database"},
		&servicev1.AccessRoute{RouteId: "stale-route", RouteClass: servicev1.RouteClass_ROUTE_CLASS_COMPUTE, DestinationDeviceId: "destination-device", DestinationExportId: exported.GetExportId(), State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: 1, ExpiresAt: expires, SourceEnvironmentId: "source", SourceContainerInstanceId: "client-instance"},
		&servicev1.RouteLease{LeaseId: "stale-lease", RouteId: "stale-route", RouteRevision: 1, Generation: 1, ExpiresAt: expires, Capability: capability}); err != nil {
		t.Fatal(err)
	}
	imports, err := database.ProjectServiceContainer(ctx, "destination", "server", "replacement-generation", "new-instance")
	if err != nil || len(imports) != 1 || imports[0] != "stale-import" {
		t.Fatalf("invalidated imports=%v err=%v", imports, err)
	}
	updatedExport, err := database.GetServiceExport(ctx, exported.GetExportId())
	if err != nil || updatedExport.GetRevoked() || updatedExport.GetRevision() != 1 || updatedExport.GetContainerGenerationId() != "replacement-generation" || updatedExport.GetContainerInstanceId() != "new-instance" {
		t.Fatalf("stale export=%+v err=%v", updatedExport, err)
	}
	updatedImport, err := database.GetServiceImport(ctx, "stale-import")
	if err != nil || updatedImport.GetRevoked() || updatedImport.GetRevision() != 1 {
		t.Fatalf("stale import=%+v err=%v", updatedImport, err)
	}
	record, err := database.GetServiceRoute(ctx, "stale-route", "stale-lease", 1)
	if err != nil || record.Route.GetState() != servicev1.RouteState_ROUTE_STATE_PREPARING || record.Route.GetGeneration() != 1 {
		t.Fatalf("stale route=%+v err=%v", record, err)
	}
	if err := database.ActivateServiceProjection(ctx, "stale-import", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ProjectServiceContainer(ctx, "source", "client", "source-replacement", "new-client"); err != nil {
		t.Fatal(err)
	}
	updatedImport, err = database.GetServiceImport(ctx, "stale-import")
	if err != nil || updatedImport.GetRevoked() || updatedImport.GetSourceContainerGenerationId() != "source-replacement" || updatedImport.GetSourceContainerInstanceId() != "new-client" {
		t.Fatal(updatedImport, err)
	}

}

func TestServiceListenerPersistenceRevocationAndRestartFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	expires := timestamppb.New(time.Now().Add(time.Hour))
	listener := &hostv1.ServiceListener{
		Id: "listener", MeshId: "mesh", DestinationDeviceId: "destination", DestinationExportId: "export",
		Exposure:      hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK,
		ListenAddress: "127.0.0.1:32100", RouteId: "route", CapabilityId: "capability", ExpiresAt: expires,
	}
	capability := &trustv1.RouteCapability{
		CapabilityId: "capability", SourceDeviceId: "source", DestinationDeviceId: "destination",
		DestinationExportId: "export", RouteClass: "device", ExpiresAt: expires,
	}
	endpoint := &trustv1.EndpointObservation{DeviceId: "destination", EndpointNodeId: []byte("node"), EndpointGeneration: 2, ExpiresAt: expires}
	created, err := database.CreateServiceListener(ctx, "request", listener, capability, endpoint)
	if err != nil || created.GetRevision() != 1 || created.GetRevoked() {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	record, err := database.GetServiceListenerByRequest(ctx, "request")
	if err != nil || record.Listener.GetId() != "listener" || record.Capability.GetRouteClass() != "device" || record.Endpoint.GetEndpointGeneration() != 2 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if duplicate, err := database.CreateServiceListener(ctx, "request", &hostv1.ServiceListener{Id: "different"}, capability, endpoint); err != nil || duplicate.GetId() != "listener" {
		t.Fatalf("idempotent create=%+v err=%v", duplicate, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.RecoverAfterRestart(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	recovered, err := reopened.GetServiceListener(ctx, "listener")
	if err != nil || !recovered.Listener.GetRevoked() || recovered.Listener.GetRevision() != 2 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	if err := reopened.RevokeServiceListener(ctx, "listener", 1); !errors.Is(err, ErrTrustRevisionConflict) {
		t.Fatalf("stale revoke error=%v", err)
	}
}

func TestEnvironmentOwnerConstraints(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	cases := []Environment{
		{ID: "local-with-remote", NamespaceID: "n1", DisplayName: "bad", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal, RegistrationID: "r", AllocationID: "a"},
		{ID: "remote-without-owner", NamespaceID: "n2", DisplayName: "bad", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerRemoteRegistration, AllocationState: 1},
	}
	for _, value := range cases {
		if _, err := store.CreateEnvironment(ctx, value); err == nil {
			t.Fatalf("invalid owner was accepted: %+v", value)
		}
	}
	remote := Environment{ID: "remote-a", NamespaceID: "rn1", DisplayName: "remote", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerRemoteRegistration, RegistrationID: "r", AllocationID: "a", AllocationState: 1}
	if _, err := store.CreateEnvironment(ctx, remote); err != nil {
		t.Fatal(err)
	}
	remote.ID, remote.NamespaceID = "remote-b", "rn2"
	if _, err := store.CreateEnvironment(ctx, remote); err == nil {
		t.Fatal("duplicate registration/allocation mapping was accepted")
	}
	if _, err := store.UpdatePolicy(ctx, "remote-a", 1, Environment{PIDs: 2, DiskBytes: 2, EgressMode: 1}); err == nil {
		t.Fatal("remote allocation policy was mutable through the local store path")
	}
}

func TestMultipleRemoteRegistrations(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	registration := RemoteRegistration{
		ID: "one", Endpoint: "controller.example:443", DisplayName: "one", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10, State: 1, ConnectionState: 1,
		EnrollmentState: 2, ConnectorPhase: 1,
	}
	created, err := store.CreateRemoteRegistration(ctx, registration)
	if err != nil || created.Revision != 1 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	registration.ID = "two"
	if _, err := store.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	registration.ID = "private"
	registration.OwnerKind = RemoteRegistrationPrivate
	registration.InstallationID = "installation"
	if _, err := store.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	registrations, err := store.ListRemoteRegistrations(ctx)
	if err != nil || len(registrations) != 3 {
		t.Fatalf("registrations=%+v err=%v", registrations, err)
	}
	page, err := store.ListRemoteRegistrationPage(ctx, "", 2)
	if err != nil || len(page) != 2 || page[0].ID != "one" || page[1].ID != "private" {
		t.Fatalf("first registration page=%+v err=%v", page, err)
	}
	page, err = store.ListRemoteRegistrationPage(ctx, page[1].ID, 2)
	if err != nil || len(page) != 1 || page[0].ID != "two" {
		t.Fatalf("second registration page=%+v err=%v", page, err)
	}
	registration.ID = "invalid-private"
	registration.InstallationID = ""
	if _, err := store.CreateRemoteRegistration(ctx, registration); err == nil {
		t.Fatal("private registration without installation was accepted")
	}
}

func TestListRemoteActiveEnvironmentsBoundedScopesAndExposesOverflow(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration := RemoteRegistration{
		ID: "one", Endpoint: "controller.example:443", PerEnvPIDs: 1, PerEnvDiskBytes: 1,
		MaxEgressMode: 1, StopGraceSeconds: 1,
		State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	}
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	registration.ID = "two"
	if _, err := database.CreateRemoteRegistration(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "local", NamespaceID: "local", OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "other", NamespaceID: "other", OwnerKind: OwnerRemoteRegistration, RegistrationID: "two", AllocationID: "other", AllocationState: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 65; index++ {
		id := fmt.Sprintf("one-%02d", index)
		if _, err := database.CreateEnvironment(ctx, Environment{ID: id, NamespaceID: id, OwnerKind: OwnerRemoteRegistration, RegistrationID: "one", AllocationID: id, AllocationState: 1, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "retained", NamespaceID: "retained", OwnerKind: OwnerRemoteRegistration, RegistrationID: "one", AllocationID: "retained", AllocationState: 4, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	environments, err := database.ListRemoteActiveEnvironmentsBounded(ctx, "one", 65)
	if err != nil || len(environments) != 65 {
		t.Fatalf("bounded environments=%d err=%v, want overflow sentinel of 65", len(environments), err)
	}
	for _, environment := range environments {
		if environment.RegistrationID != "one" || environment.OwnerKind != OwnerRemoteRegistration || environment.AllocationID == "retained" {
			t.Fatalf("bounded query crossed registration scope: %+v", environment)
		}
	}
	if _, err := database.ListRemoteActiveEnvironmentsBounded(ctx, "one", 0); err == nil {
		t.Fatal("non-positive query bound was accepted")
	}
}

func TestEnvironmentPolicyAndSessionPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	environment, err := store.CreateEnvironment(ctx, Environment{
		ID: "environment-a", NamespaceID: "namespace-a", DisplayName: "Agent A",
		PIDs: 128, DiskBytes: 4 << 30, EgressMode: 1, OwnerKind: OwnerLocal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if environment.PolicyRevision != 1 {
		t.Fatalf("revision = %d", environment.PolicyRevision)
	}
	updated, err := store.UpdatePolicy(ctx, environment.ID, 1, Environment{
		PIDs: 256, DiskBytes: 8 << 30, EgressMode: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.PolicyRevision != 2 || updated.PIDs != 256 {
		t.Fatalf("updated = %+v", updated)
	}
	if _, err := store.UpdatePolicy(ctx, environment.ID, 1, Environment{}); err == nil {
		t.Fatal("stale revision was accepted")
	}
	if err := store.PutVolumeOwner(ctx, "volume-a", environment.NamespaceID); err != nil {
		t.Fatal(err)
	}
	if exists, err := store.HasVolumePolicy(ctx, "volume-a", environment.NamespaceID); err != nil || !exists {
		t.Fatalf("volume policy exists=%t, err=%v", exists, err)
	}
	expires := time.Now().Add(time.Hour).Truncate(time.Nanosecond)
	if err := store.CreateSession(ctx, Session{ID: "session-a", EnvironmentID: environment.ID, SocketPath: "/tmp/a.sock", ExpiresAt: expires}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.RecoverAfterRestart(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	session, err := store.GetSession(ctx, "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if session.NamespaceID != "namespace-a" || !session.ExpiresAt.Equal(expires) {
		t.Fatalf("session = %+v", session)
	}
	if session.RevokedAt == nil || session.TerminalReason != "SIDECAR_RESTARTED" {
		t.Fatalf("live session survived host restart: %+v", session)
	}
	if err := store.RevokeSession(ctx, session.ID); err != nil {
		t.Fatalf("revoke after restart error = %v", err)
	}
}

func TestAgentIntegrationOwnsSessionAuthorizationAndRemovalKeepsEnvironment(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "agent-data", NamespaceID: "agent-namespace", DisplayName: "Agent data",
		PIDs: 128, DiskBytes: 4 << 30,
		EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "integration-a", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "/tmp/config", EnvironmentID: "agent-data", DesiredState: 1,
		ConfigState: 1, PrepareRequestID: "prepare-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := database.CommitAgentIntegration(ctx, "commit-store_test-547", prepared.ID, prepared.Revision, "fingerprint", 3)
	if err != nil || waiting.ConfigState != 3 || waiting.Revision != 2 {
		t.Fatalf("waiting=%+v err=%v", waiting, err)
	}
	if err := database.VerifyAgentIntegration(ctx, waiting.ID, waiting.Revision); err != nil {
		t.Fatal(err)
	}
	ready, err := database.GetAgentIntegration(ctx, waiting.ID)
	if err != nil || ready.ConfigState != 2 || ready.Revision != waiting.Revision || ready.LastVerifiedAt == nil {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	previousRevision := ready.Revision
	ready, err = database.CommitAgentIntegration(ctx, "commit-store_test-559", ready.ID, ready.Revision, "updated-image", 2)
	if err != nil || ready.ConfigFingerprint != "updated-image" || ready.Revision != previousRevision+1 {
		t.Fatalf("configuration update=%+v err=%v", ready, err)
	}
	if _, err := database.CommitAgentIntegration(ctx, "commit-store_test-563", ready.ID, previousRevision, "stale-image", 2); err == nil {
		t.Fatal("stale configuration update was accepted")
	}
	createdAt := time.Now().Add(-time.Minute).Truncate(time.Nanosecond)
	if err := database.CreateSession(ctx, Session{
		ID: "session-a", IntegrationID: ready.ID, EnvironmentID: ready.EnvironmentID,
		SocketPath: "/tmp/integration.sock", Origin: 1, CreatedAt: createdAt, ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.MarkSessionConnected(ctx, "session-a", "", 0); err != nil {
		t.Fatal(err)
	}
	sessions, err := database.ListSessions(ctx, ready.ID, false)
	if err != nil || len(sessions) != 1 || sessions[0].IntegrationID != ready.ID || sessions[0].ConnectedAt == nil {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+ready.ID, ready.ID, ready.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+ready.ID, ready.ID, ready.Revision); err != nil || replay.Revision != removing.Revision {
		t.Fatalf("begin replay=%+v err=%v", replay, err)
	}
	if _, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+ready.ID, ready.ID, ready.Revision+1); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatalf("changed request = %v", err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+ready.ID, ready.ID, removing.Revision); err == nil {
		t.Fatal("completed with active session")
	}
	if err := database.RevokeSession(ctx, "session-a"); err != nil {
		t.Fatal(err)
	}
	removed, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+ready.ID, ready.ID, removing.Revision)
	if err != nil || removed.DesiredState != 2 || removed.ConfigState != 7 {
		t.Fatalf("removed=%+v err=%v", removed, err)
	}
	if replay, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+ready.ID, ready.ID, removing.Revision); err != nil || replay.Revision != removed.Revision {
		t.Fatalf("complete replay=%+v err=%v", replay, err)
	}
	if _, err := database.CommitAgentIntegration(ctx, "commit-store_test-603", removed.ID, removed.Revision, "revive", 2); err == nil {
		t.Fatal("configuration update revived a removed integration")
	}
	if _, err := database.GetEnvironment(ctx, "agent-data"); err != nil {
		t.Fatalf("access removal deleted retained environment: %v", err)
	}
}

func TestAgentSessionAuthorizationIsAtomicWithRemoval(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "Environment", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: "environment", DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := database.CommitAgentIntegration(ctx, "commit-store_test-625", prepared.ID, prepared.Revision, "fingerprint", 2)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- database.CreateAuthorizedSession(ctx, Session{ID: "session", IntegrationID: ready.ID, EnvironmentID: ready.EnvironmentID, SocketPath: "/tmp/session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, ready.Revision)
	}()
	go func() {
		<-start
		_, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+ready.ID, ready.ID, ready.Revision)
		results <- err
	}()
	close(start)
	first, second := <-results, <-results
	integration, err := database.GetAgentIntegration(ctx, ready.ID)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := database.ListSessions(ctx, ready.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if integration.ConfigState == 6 && len(sessions) == 0 && first == nil && second == nil {
		t.Fatal("removal and authorization both succeeded without a revocable session")
	}
}

func TestCancelledPendingIntegrationPreservesOwnerUntilDiscard(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	integrationID := "cancelled"
	environmentID := "integration-" + integrationID
	if _, err := database.CreateEnvironment(ctx, Environment{ID: environmentID, NamespaceID: "namespace", DisplayName: "Pending", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: integrationID, AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: environmentID, DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+prepared.ID, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+prepared.ID, prepared.ID, removing.Revision); err != nil {
		t.Fatal(err)
	}
	if current, err := database.GetAgentIntegration(ctx, prepared.ID); err != nil || current.ConfigState != 7 {
		t.Fatalf("removed owner=%+v err=%v", current, err)
	}
	if replay, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+prepared.ID, prepared.ID, removing.Revision); err != nil || replay.ID != prepared.ID || replay.ConfigState != 7 {
		t.Fatalf("retained owner receipt=%+v err=%v", replay, err)
	}
	if _, _, err := database.CheckAgentIntegrationRemoval(ctx, "complete-"+prepared.ID, "different-owner", removing.Revision); !errors.Is(err, ErrOperationReceiptConflict) {
		t.Fatalf("exact owner conflict=%v", err)
	}

	if _, err := database.GetEnvironment(ctx, environmentID); err != nil {
		t.Fatalf("environment was deleted: %v", err)
	}
	if err := database.DiscardEnvironment(ctx, environmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetAgentIntegration(ctx, prepared.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("explicit discard retained owner: %v", err)
	}
}

func TestCancelledIntegrationCleanupRollsBackWhenEnvironmentIsReferenced(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	integrationID, environmentID := "blocked", "integration-blocked"
	if _, err := database.CreateEnvironment(ctx, Environment{ID: environmentID, NamespaceID: "namespace", DisplayName: "Pending", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: integrationID, AdapterID: "codex", DisplayName: "Codex", Scope: 1, ConfigTarget: "config", EnvironmentID: environmentID, DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare-blocked"})
	if err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+prepared.ID, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{ID: "reference", IntegrationID: integrationID, EnvironmentID: environmentID, SocketPath: "/tmp/reference", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+integrationID, integrationID, removing.Revision); err == nil {
		t.Fatal("referenced cancellation succeeded")
	}
	current, err := database.GetAgentIntegration(ctx, integrationID)
	if err != nil || current.ConfigState != 6 {
		t.Fatalf("integration did not roll back: %+v err=%v", current, err)
	}
	if _, err := database.GetEnvironment(ctx, environmentID); err != nil {
		t.Fatalf("environment was partially deleted: %v", err)
	}
}

func TestRemoteEnrollmentTransitionsAreRevisionAndStateFenced(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	pending, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", DisplayName: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State: 1, ConnectionState: 1, EnrollmentState: 1, ConnectorPhase: 2, EnrollmentRequestID: "begin-registration",
	})
	if err != nil {
		t.Fatal(err)
	}
	byRequest, err := database.RemoteRegistrationByEnrollmentRequest(ctx, "begin-registration")
	if err != nil || byRequest.ID != pending.ID {
		t.Fatalf("enrollment request lookup=%+v err=%v", byRequest, err)
	}
	incomplete, err := database.SetRemoteEnrollment(ctx, pending.ID, pending.Revision, 1, 3, "KEYCHAIN_WRITE_FAILED", "Credential storage failed.")
	if err != nil || incomplete.EnrollmentState != 3 || incomplete.EnrollmentErrorCode != "KEYCHAIN_WRITE_FAILED" {
		t.Fatalf("incomplete=%+v err=%v", incomplete, err)
	}
	if _, err := database.SetRemoteEnrollment(ctx, pending.ID, pending.Revision, 1, 2, "", ""); err == nil {
		t.Fatal("stale enrollment revision was accepted")
	}
	retrying, err := database.RetryRemoteEnrollment(ctx, incomplete.ID, incomplete.Revision)
	if err != nil {
		t.Fatal(err)
	}
	retrying, err = database.RetryRemoteEnrollment(ctx, retrying.ID, retrying.Revision)
	if err != nil || retrying.EnrollmentState != 1 {
		t.Fatalf("pending enrollment was not recoverable: %+v err=%v", retrying, err)
	}
	ready, err := database.SetRemoteEnrollment(ctx, retrying.ID, retrying.Revision, 1, 2, "", "")
	if err != nil || ready.EnrollmentState != 2 {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if _, err := database.SetRemoteEnrollment(ctx, ready.ID, ready.Revision, 1, 3, "FAILED", "invalid transition"); err == nil {
		t.Fatal("invalid enrollment transition was accepted")
	}
	revoking, err := database.BeginRemoteRevocation(ctx, ready.ID, ready.Revision)
	if err != nil || revoking.State != 2 || revoking.EnrollmentState != 4 || revoking.ConnectionState != 1 || revoking.ConnectionInstance != "" {
		t.Fatalf("revoking=%+v err=%v", revoking, err)
	}
	retry, err := database.BeginRemoteRevocation(ctx, revoking.ID, revoking.Revision)
	if err != nil || retry.Revision != revoking.Revision {
		t.Fatalf("revocation retry = %+v, %v", retry, err)
	}
	if _, err := database.BeginRemoteRevocation(ctx, revoking.ID, ready.Revision); err == nil {
		t.Fatal("stale revocation retry accepted")
	}
	revoked, err := database.CompleteRemoteRevocation(ctx, revoking.ID, revoking.Revision)
	if err != nil || revoked.State != 3 || revoked.ConnectionState != 1 {
		t.Fatalf("revoked=%+v err=%v", revoked, err)
	}
	if _, err := database.SetRemoteRegistrationState(ctx, revoked.ID, revoked.Revision, 1); err == nil {
		t.Fatal("revoked registration was enabled")
	}

}

func TestConnectorStatusRejectsStaleInstances(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{
		ID: "registration", Endpoint: "controller.example:443", PerEnvPIDs: 1, PerEnvDiskBytes: 1,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	connected, err := database.SetRemoteConnection(ctx, registration.ID, "current", 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "", 3, "test", time.Now(), time.Time{}, 0, "", ""); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("empty stale instance report error=%v", err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "old", 3, "test", time.Now(), time.Time{}, 0, "", ""); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("replaced instance report error=%v", err)
	}
	if _, err := database.ReportConnectorStatus(ctx, connected.ID, "current", 3, "test", time.Now(), time.Time{}, 0, "", ""); err != nil {
		t.Fatalf("current instance report error=%v", err)
	}
	disconnected, err := database.SetRemoteConnection(ctx, connected.ID, "current", 1)
	if err != nil || disconnected.ConnectionInstance != "" {
		t.Fatalf("disconnect did not clear fence: %+v err=%v", disconnected, err)
	}
	if _, err := database.ReportConnectorStatus(ctx, disconnected.ID, "", 5, "test", time.Now(), time.Time{}, 1, "CONNECTION_FAILED", "retrying"); err != nil {
		t.Fatalf("disconnected status report error=%v", err)
	}
}

func TestRemotePoolPolicyRevisionIsIndependentFromLifecycleRevision(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{
		ID: "policy-revision", Endpoint: "controller.example:443", PerEnvPIDs: 1, PerEnvDiskBytes: 1,
		MaxEgressMode: 1, StopGraceSeconds: 10,
		State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if registration.PoolPolicyRevision != 1 {
		t.Fatalf("initial pool policy revision = %d", registration.PoolPolicyRevision)
	}
	draining, err := database.SetRemoteRegistrationState(ctx, registration.ID, registration.Revision, 2)
	if err != nil {
		t.Fatal(err)
	}
	if draining.Revision == registration.Revision || draining.PoolPolicyRevision != registration.PoolPolicyRevision {
		t.Fatalf("lifecycle transition changed the wrong revision: before=%+v after=%+v", registration, draining)
	}
	updated := draining
	updated.PerEnvPIDs = 2
	updated, err = database.UpdateRemotePool(ctx, updated, draining.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision != draining.Revision+1 || updated.PoolPolicyRevision != draining.PoolPolicyRevision+1 {
		t.Fatalf("pool update revisions: before=%+v after=%+v", draining, updated)
	}
}

func TestOperationRequestIDDeduplication(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	first, inserted, err := store.PutOperation(ctx, Operation{ID: "operation-a", Kind: "diagnostic", EnvironmentID: "system", State: 1, Stage: "queued", RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first operation was not inserted")
	}
	second, inserted, err := store.PutOperation(ctx, Operation{ID: "operation-b", Kind: "diagnostic", EnvironmentID: "system", State: 1, Stage: "queued", RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("duplicate operation was inserted")
	}
	if first.ID != second.ID {
		t.Fatalf("dedupe returned %q and %q", first.ID, second.ID)
	}
}

func TestCanceledOperationInsertDoesNotCreateAcceptanceRecord(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = store.PutOperation(ctx, Operation{ID: "operation-a", Kind: "cleanup", EnvironmentID: "environment-a", State: 1, Stage: "started", RequestID: "request-a"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled operation insert err=%v", err)
	}
	if _, err := store.OperationByRequestID(context.Background(), "request-a"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("canceled operation created an acceptance record: %v", err)
	}
}

func TestOpenPreservesResumableOperationsAndInterruptsLostHandlers(t *testing.T) {
	for _, kind := range []string{"diagnostic", "local-disposal", "host-share-create", "host-share-rebind", "host-share-revoke"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.sqlite")
			first, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			created, _, err := first.PutOperation(context.Background(), Operation{ID: "operation-a", Kind: kind, State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "accepted", RequestID: "request-a"})
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			second, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			if err := second.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
				t.Fatal(err)
			}
			recovered, err := second.GetOperation(context.Background(), created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "diagnostic" {
				if recovered.State != int32(hostv1.OperationState_OPERATION_STATE_INTERRUPTED) || recovered.ErrorReason != "SIDECAR_RESTARTED" {
					t.Fatalf("lost handler operation = %+v", recovered)
				}
			} else if recovered.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || recovered.Stage != "accepted" || recovered.ErrorReason != "" {
				t.Fatalf("resumable operation = %+v", recovered)
			}
		})
	}
}

func TestRevokedGrantRemainsAsReconciliationTombstone(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.PutGrant(ctx, Grant{ID: "link-a", Kind: "network-link", SourceNamespaceID: "source", TargetNamespaceID: "target", Payload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeGrant(ctx, "link-a"); err != nil {
		t.Fatal(err)
	}
	if grants, err := store.ActiveGrants(ctx); err != nil || len(grants) != 0 {
		t.Fatalf("active grants after revoke = %v, %v", grants, err)
	}
	grants, err := store.AllGrants(ctx)
	if err != nil || len(grants) != 1 || !grants[0].Revoked {
		t.Fatalf("grant tombstones = %v, %v", grants, err)
	}
	if err := store.CompactRevokedGrant(ctx, "link-a"); err != nil {
		t.Fatal(err)
	}
	if grants, err := store.AllGrants(ctx); err != nil || len(grants) != 0 {
		t.Fatalf("compacted grant tombstones = %v, %v", grants, err)
	}
}

func TestActiveGrantsForNamespaceIsScopedAndReturnsOneOverflowRecord(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, grant := range []Grant{
		{ID: "a", Kind: "network-link", SourceNamespaceID: "selected", TargetNamespaceID: "other", Payload: "{}"},
		{ID: "b", Kind: "network-link", SourceNamespaceID: "other", TargetNamespaceID: "selected", Payload: "{}"},
		{ID: "c", Kind: "network-link", SourceNamespaceID: "selected", TargetNamespaceID: "other-2", Payload: "{}"},
		{ID: "unrelated", Kind: "network-link", SourceNamespaceID: "other", TargetNamespaceID: "other-2", Payload: "{}"},
	} {
		if err := database.PutGrant(ctx, grant); err != nil {
			t.Fatal(err)
		}
	}
	grants, err := database.ActiveGrantsForNamespace(ctx, "selected", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 || grants[0].ID != "a" || grants[1].ID != "b" {
		t.Fatalf("bounded grants=%+v", grants)
	}
}

func TestGrantsForNamespaceIncludesScopedTombstonesAndReturnsOneOverflowRecord(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, grant := range []Grant{
		{ID: "active", Kind: "network-link", SourceNamespaceID: "selected", TargetNamespaceID: "other", Payload: "{}"},
		{ID: "revoked", Kind: "network-link", SourceNamespaceID: "other", TargetNamespaceID: "selected", Payload: "{}"},
		{ID: "unrelated", Kind: "network-link", SourceNamespaceID: "other", TargetNamespaceID: "other-2", Payload: "{}"},
	} {
		if err := database.PutGrant(ctx, grant); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.RevokeGrant(ctx, "revoked"); err != nil {
		t.Fatal(err)
	}
	grants, err := database.GrantsForNamespace(ctx, "selected", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 || grants[0].ID != "active" || grants[0].Revoked || grants[1].ID != "revoked" || !grants[1].Revoked {
		t.Fatalf("bounded grants=%+v", grants)
	}
}

func TestDiscardEnvironmentRemovesOnlyFullyRemovedIntegrationHistory(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "retained", NamespaceID: "retained-namespace", DisplayName: "Retained",
		PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	prepared, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "removed-integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "config", EnvironmentID: "retained", DesiredState: 1, ConfigState: 2,
		PrepareRequestID: "prepare-removed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{
		ID: "historical-session", IntegrationID: prepared.ID, EnvironmentID: "retained",
		SocketPath: "/tmp/historical-session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, "begin-"+prepared.ID, prepared.ID, prepared.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RevokeSession(ctx, "historical-session"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteAgentIntegrationRemoval(ctx, "complete-"+removing.ID, removing.ID, removing.Revision); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "retained"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetEnvironment(ctx, "retained"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("discarded environment lookup = %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, prepared.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removed integration survived discard: %v", err)
	}
	if _, err := database.GetSession(ctx, "historical-session"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("historical session survived discard: %v", err)
	}
}

func TestDiscardEnvironmentRejectsActiveIntegrationWithoutMutation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "active", NamespaceID: "active-namespace", DisplayName: "Active",
		PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAgentIntegration(ctx, AgentIntegration{
		ID: "active-integration", AdapterID: "codex", DisplayName: "Codex", Scope: 1,
		ConfigTarget: "config", EnvironmentID: "active", DesiredState: 1, ConfigState: 1,
		PrepareRequestID: "prepare-active",
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "active"); !errors.Is(err, ErrEnvironmentHasAgentAccess) {
		t.Fatalf("discard active integration error = %v", err)
	}
	if _, err := database.GetEnvironment(ctx, "active"); err != nil {
		t.Fatalf("blocked discard removed environment: %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, "active-integration"); err != nil {
		t.Fatalf("blocked discard removed integration: %v", err)
	}
}

func TestDiscardEnvironmentRequiresRetainedRemoteAllocation(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	states := []hostv1.AllocationState{
		hostv1.AllocationState_ALLOCATION_STATE_READY,
		hostv1.AllocationState_ALLOCATION_STATE_DRAINING,
	}
	for index, state := range states {
		id := fmt.Sprintf("remote-active-%d", index)
		if _, err := database.CreateEnvironment(ctx, Environment{
			ID: id, NamespaceID: id + "-namespace", DisplayName: id,
			PIDs: 1, DiskBytes: 1, EgressMode: 1,
			OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: id,
			AllocationState: int32(state),
		}); err != nil {
			t.Fatal(err)
		}
		if err := database.ValidateEnvironmentDiscard(ctx, id); !errors.Is(err, ErrRemoteEnvironmentActive) {
			t.Fatalf("validate state %v error = %v", state, err)
		}
		if err := database.DiscardEnvironment(ctx, id); !errors.Is(err, ErrRemoteEnvironmentActive) {
			t.Fatalf("discard state %v error = %v", state, err)
		}
		if _, err := database.GetEnvironment(ctx, id); err != nil {
			t.Fatalf("blocked discard removed state %v environment: %v", state, err)
		}
	}
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "remote-retained", NamespaceID: "remote-retained-namespace", DisplayName: "retained",
		PIDs: 1, DiskBytes: 1, EgressMode: 1,
		OwnerKind: OwnerRemoteRegistration, RegistrationID: "registration", AllocationID: "retained",
		AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED),
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "remote-retained"); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPreservesRetainedRemoteAllocation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller", DisplayName: "controller", PerEnvPIDs: 256, PerEnvDiskBytes: 4 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 30, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	contract := resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 2 << 30}
	created, err := database.BeginWorkloadAdmission(ctx, Environment{
		ID: "remote-leased", NamespaceID: "remote-leased-namespace", DisplayName: "leased", OwnerKind: OwnerRemoteRegistration,
		RegistrationID: "registration", AllocationID: "allocation", AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY), AllocationGeneration: 7, PIDs: uint32(contract.PIDMax), DiskBytes: contract.WritableQuotaBytes},
		resourcev2.Contract{PIDMax: 256, WritableQuotaBytes: 4 << 30})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	created, err = database.SetAllocationState(ctx, created.ID, created.PolicyRevision, int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED))
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.RecoverAfterRestart(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	recovered, err := reopened.GetEnvironment(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED) {
		t.Fatalf("allocation state after restart = %s", hostv1.AllocationState(recovered.AllocationState))
	}
	if recovered.PolicyRevision != created.PolicyRevision {
		t.Fatalf("policy revision after restart = %d, want %d", recovered.PolicyRevision, created.PolicyRevision)
	}
	if recovered.AllocationGeneration != 7 || recovered.Resources() != contract {
		t.Fatalf("restart changed durable allocation: %+v", recovered)
	}

}

func TestDiscardEnvironmentRollsBackRelatedHistoryOnFailure(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{
		ID: "rollback", NamespaceID: "rollback-namespace", DisplayName: "Rollback",
		PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `INSERT INTO agent_integrations(
		id, adapter_id, display_name, scope, config_target, environment_id, desired_state, config_state,
		created_at, updated_at, prepare_request_id) VALUES ('removed', 'codex', 'Codex', 1, 'config', 'rollback', 2, 7, 1, 1, 'prepare')`); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{
		ID: "rollback-session", IntegrationID: "removed", EnvironmentID: "rollback",
		SocketPath: "/tmp/rollback-session", Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER reject_environment_delete BEFORE DELETE ON environments
		WHEN OLD.id='rollback' BEGIN SELECT RAISE(ABORT, 'injected discard failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := database.DiscardEnvironment(ctx, "rollback"); err == nil {
		t.Fatal("discard unexpectedly succeeded")
	}
	if _, err := database.GetEnvironment(ctx, "rollback"); err != nil {
		t.Fatalf("rollback lost environment: %v", err)
	}
	if _, err := database.GetAgentIntegration(ctx, "removed"); err != nil {
		t.Fatalf("rollback lost integration: %v", err)
	}
	if _, err := database.GetSession(ctx, "rollback-session"); err != nil {
		t.Fatalf("rollback lost session: %v", err)
	}
}

func TestSessionTerminalReplaysPreserveFirstReasonAndRejectMissing(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprint(closed), func(t *testing.T) {
			database, err := Open(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			environment := Environment{ID: "terminal-env", NamespaceID: "terminal-ns", OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}
			if _, err := database.CreateEnvironment(ctx, environment); err != nil {
				t.Fatal(err)
			}
			if err := database.CreateSession(ctx, Session{ID: "terminal-session", EnvironmentID: environment.ID, SocketPath: "/tmp/test.sock", CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			if err := database.EndSession(ctx, "terminal-session", "FIRST", closed); err != nil {
				t.Fatal(err)
			}
			original, err := database.GetSession(ctx, "terminal-session")
			if err != nil {
				t.Fatal(err)
			}
			for _, again := range []bool{false, true, false} {
				if err := database.EndSession(ctx, "terminal-session", "LATER", again); err != nil {
					t.Fatal(err)
				}
			}
			current, err := database.GetSession(ctx, "terminal-session")
			if err != nil {
				t.Fatal(err)
			}
			if current.TerminalReason != "FIRST" || !reflect.DeepEqual(current.RevokedAt, original.RevokedAt) || !reflect.DeepEqual(current.ClosedAt, original.ClosedAt) {
				t.Fatalf("terminal state changed: %+v -> %+v", original, current)
			}
			if err := database.RevokeSession(ctx, "missing-session"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("missing = %v", err)
			}
		})
	}
}

func TestPartialServiceRemovalPreservesOwnerForReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	environment := "integration-partial-service"
	if _, err := database.CreateEnvironment(ctx, Environment{ID: environment, NamespaceID: "partial-namespace", DisplayName: "Partial", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	owner, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "partial-service", AdapterID: "client-service", DisplayName: "gitea", Scope: 1, ConfigTarget: "image", EnvironmentID: environment, DesiredState: 1, ConfigState: 1, PrepareRequestID: "prepare-partial"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateServiceExport(ctx, "partial-export", &servicev1.ServiceExport{ExportId: "partial-export", EnvironmentId: environment, ContainerId: "gitea", ContainerGenerationId: "generation", ContainerInstanceId: "instance", Port: 3000, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP}); err != nil {
		t.Fatal(err)
	}
	if err := database.SetManagedContainerRunning(ctx, environment, "gitea", "generation", true); err != nil {
		t.Fatal(err)
	}
	removing, err := database.BeginAgentIntegrationRemoval(ctx, "partial-begin", owner.ID, owner.Revision)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := database.GetServiceExport(ctx, "partial-export")
	if err != nil || !exported.GetRevoked() {
		t.Fatal("removing integration retained running authority", exported, err)
	}
	managed, err := database.GetManagedContainer(ctx, environment, "gitea")
	if err != nil || managed.DesiredRunning {
		t.Fatal("removal retained container running intent", managed, err)
	}
	removed, err := database.CompleteAgentIntegrationRemoval(ctx, "partial-complete", owner.ID, removing.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	replay, err := database.CompleteAgentIntegrationRemoval(ctx, "partial-complete", owner.ID, removing.Revision)
	if err != nil || replay.Revision != removed.Revision {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	current, err := database.GetAgentIntegration(ctx, owner.ID)
	if err != nil || current.ConfigState != 7 {
		t.Fatalf("retained owner=%+v err=%v", current, err)
	}
	if _, err := database.GetEnvironment(ctx, environment); err != nil {
		t.Fatalf("retained environment=%v", err)
	}
	if err := database.ValidateEnvironmentDiscard(ctx, environment); err != nil {
		t.Fatalf("removed access still blocks discard=%v", err)
	}
}

func TestRemoteEnrollmentRestartPreservesPrivatePreparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, kind := range []int32{RemoteRegistrationExternal, RemoteRegistrationPrivate} {
		id := fmt.Sprintf("registration-%d", kind)
		installationID := ""
		if kind == RemoteRegistrationPrivate {
			installationID = "installation"
		}
		_, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: id, Endpoint: "controller.example:443", DisplayName: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 10, State: 1, ConnectionState: 1, EnrollmentState: 1, ConnectorPhase: 2, OwnerKind: kind, InstallationID: installationID})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.RecoverAfterRestart(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	external, err := database.GetRemoteRegistration(ctx, "registration-1")
	if err != nil || external.EnrollmentState != 3 || external.EnrollmentErrorCode != "ENROLLMENT_INTERRUPTED" {
		t.Fatalf("external interrupted enrollment=%+v %v", external, err)
	}
	private, err := database.GetRemoteRegistration(ctx, "registration-2")
	if err != nil || private.EnrollmentState != 1 || private.EnrollmentErrorCode != "" || private.ConnectionState != 1 {
		t.Fatalf("private preparation lost after restart=%+v %v", private, err)
	}
}

func TestRestartRecoveryRollsBackAllOwners(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	if _, err := database.CreateEnvironment(ctx, Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "test", PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: OwnerLocal}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateSession(ctx, Session{ID: "session", EnvironmentID: "environment", SocketPath: "session.sock", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.PutOperation(ctx, Operation{ID: "operation", Kind: "diagnostic", RequestID: "request", State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING)}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateRemoteRegistration(ctx, RemoteRegistration{ID: "registration", Endpoint: "controller.example:443", DisplayName: "test", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 1, ConnectorPhase: 1, OwnerKind: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`CREATE TRIGGER reject_recovery BEFORE UPDATE ON remote_registrations BEGIN SELECT RAISE(ABORT,'injected recovery failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := database.RecoverAfterRestart(ctx, time.Now()); err == nil {
		t.Fatal("recovery unexpectedly succeeded")
	}
	operation, err := database.GetOperation(ctx, "operation")
	if err != nil || operation.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
		t.Fatalf("operation changed: %+v, %v", operation, err)
	}
	session, err := database.GetSession(ctx, "session")
	if err != nil || session.RevokedAt != nil {
		t.Fatalf("session changed: %+v, %v", session, err)
	}
}
