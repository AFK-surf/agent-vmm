//go:build darwin

package host

import (
	"context"
	"database/sql"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestManagedRegistrationMatchBindsIdentityTrustScopeAndPolicy(t *testing.T) {
	cpu, memory, pids, disk := uint32(1000), uint64(1<<30), uint32(128), uint64(2<<30)
	managed := &hostv1.BeginRemoteEnrollmentRequest{
		RegistrationId: "registration-1", Endpoint: "gateway.example.test:7443", DisplayName: "Managed",
		ProtocolVersion: "1", TrustBundle: []byte("trust"), ScopeDigest: "scope", SourcePolicyRevision: 7,
		Pool: &hostv1.RemotePoolPolicy{
			PerEnvironmentLimits: &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk},
			MaxEgressMode:        hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StopGrace: durationpb.New(time.Second)},
	}
	stored := store.RemoteRegistration{ID: managed.RegistrationId, Endpoint: managed.Endpoint, DisplayName: managed.DisplayName,
		ProtocolVersion: managed.ProtocolVersion, TrustBundle: append([]byte(nil), managed.TrustBundle...), ScopeDigest: managed.ScopeDigest,
		SourcePolicyRevision: managed.SourcePolicyRevision, PerEnvPIDs: pids,
		PerEnvDiskBytes: disk, MaxEgressMode: int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL), StopGraceSeconds: 1}
	capacity := store.Environment{CPUMillis: cpu, MemoryBytes: memory, PIDs: pids, DiskBytes: disk}

	if matches, err := managedRegistrationMatches(managed, stored, capacity); err != nil || !matches {
		t.Fatalf("exact managed registration did not match: matches=%v err=%v", matches, err)
	}
	stored.TrustBundle = []byte("other")
	if matches, err := managedRegistrationMatches(managed, stored, capacity); err != nil || matches {
		t.Fatalf("trust drift was accepted: matches=%v err=%v", matches, err)
	}
	*managed.Pool.PerEnvironmentLimits.CpuMillis = cpu + 1
	if _, err := registrationFromPool(managed.Pool, capacity); err != nil {
		t.Fatalf("CPU template value constrained shared capacity: %v", err)
	}
	*managed.Pool.PerEnvironmentLimits.Pids = pids + 1
	if _, err := registrationFromPool(managed.Pool, capacity); err == nil {
		t.Fatal("PID limit above host capacity was accepted")
	}
}

func TestExactRegistrationReadSeparatesAbsentFromUnreadable(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	server := &operatorServer{controller: &Controller{store: database}}
	read := func() error {
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: "missing"})
		})
		return err
	}
	if err := read(); status.Code(err) != codes.NotFound {
		t.Fatalf("absent = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read(); status.Code(err) != codes.Unavailable {
		t.Fatalf("unreadable = %v", err)
	}
}

func TestDisposedRegistrationControlReleasesAdmissionAndKeepsTypedAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	registration, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "disposed", Endpoint: "controller", ProtocolVersion: "2", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 5, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1, EnrollmentRequestID: "install-enrollment"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.AcceptLocalDisposal(ctx, store.Operation{ID: "dispose", RequestID: "confirm"}, store.LocalDisposal{Kind: "registration", TargetID: registration.ID, Revision: registration.Revision}); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	server := &operatorServer{controller: controller}
	for attempt := 0; attempt < 2; attempt++ {
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.SetRemoteRegistrationState(ctx, &hostv1.SetRemoteRegistrationStateRequest{RegistrationId: registration.ID, ExpectedRevision: registration.Revision, RequestId: "old-control", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED})
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("disposed control: %v", err)
		}
		if !controller.remoteAdmissionMu.TryLock() {
			t.Fatal("rejected control kept admission locked for other registrations")
		}
		controller.remoteAdmissionMu.Unlock()
	}
	root, err := database.GetLocalDisposal(ctx, "registration", registration.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishLocalDisposal(ctx, root); err != nil {
		t.Fatal(err)
	}
	// Model retirement of the old metadata row. The owner disposal fence must still exist.
	fixture, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	fixture.SetMaxOpenConns(1)
	if _, err := fixture.Exec("DELETE FROM remote_registrations WHERE id=?", registration.ID); err != nil {
		t.Fatal(err)
	}
	_, err = auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return server.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registration.ID})
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("disposed absence: %v", err)
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "LOCAL_DISPOSED" {
			return
		}
	}
	t.Fatal("disposed absence lost its terminal reason")
}
