//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
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
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestRepeatedManagedEnrollmentUpdatesPolicyAndRejectsIdentityChange(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	scope := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	stored, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "managed", Endpoint: "gateway.example.test:7443", DisplayName: "Old name",
		ProtocolVersion: "1", TrustBundle: []byte("trust"), ScopeDigest: scope, SourcePolicyRevision: 7, PerEnvPIDs: 128, PerEnvDiskBytes: 2 << 30,
		MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1, EnrollmentRequestID: "install-1"})
	if err != nil {
		t.Fatal(err)
	}
	server := &operatorServer{controller: &Controller{store: database, remoteCapacity: store.Environment{PIDs: 1024, DiskBytes: 8 << 30}}}
	pids, disk := uint32(256), uint64(4<<30)
	request := func(requestID string, revision uint64, mutate func(*hostv1.BeginRemoteEnrollmentRequest)) error {
		managed := &hostv1.BeginRemoteEnrollmentRequest{RequestId: requestID, RegistrationId: stored.ID, Endpoint: stored.Endpoint, DisplayName: "New name",
			ProtocolVersion: remoteconnector.ProtocolVersion, TrustBundle: []byte("trust"), ScopeDigest: scope, SourcePolicyRevision: revision,
			Pool: &hostv1.RemotePoolPolicy{PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk},
				MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(30 * time.Second)}}
		if mutate != nil {
			mutate(managed)
		}
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.ApplyManagedRemoteEnrollment(ctx, &hostv1.ApplyManagedRemoteEnrollmentRequest{Registration: managed, EnrollmentToken: []byte("token")})
		})
		return err
	}
	reason := func(err error) string {
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				return info.Reason
			}
		}
		return ""
	}

	if err := request("install-2", 8, nil); err != nil {
		t.Fatal(err)
	}
	updated, err := database.GetRemoteRegistration(ctx, stored.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.DisplayName != "New name" || updated.SourcePolicyRevision != 8 || updated.PerEnvPIDs != pids || updated.PerEnvDiskBytes != disk ||
		updated.MaxEgressMode != int32(hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET) || updated.StopGraceSeconds != 30 ||
		updated.PoolPolicyRevision != stored.PoolPolicyRevision+1 || updated.ProtocolVersion != remoteconnector.ProtocolVersion || updated.EnrollmentState != stored.EnrollmentState {
		t.Fatalf("registration was not updated in place: %+v", updated)
	}
	if err := request("install-3", 7, nil); reason(err) != "POLICY_REVISION_CONFLICT" {
		t.Fatalf("older policy revision = %v", err)
	}
	identity := map[string]func(*hostv1.BeginRemoteEnrollmentRequest){
		"endpoint": func(managed *hostv1.BeginRemoteEnrollmentRequest) { managed.Endpoint = "other.example.test:7443" },
		"trust":    func(managed *hostv1.BeginRemoteEnrollmentRequest) { managed.TrustBundle = []byte("other") },
		"scope": func(managed *hostv1.BeginRemoteEnrollmentRequest) {
			managed.ScopeDigest = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
		},
	}
	for name, mutate := range identity {
		if err := request("install-"+name, 9, mutate); reason(err) != "REGISTRATION_IDENTITY_MISMATCH" {
			t.Fatalf("%s change = %v", name, err)
		}
	}
	if current, err := database.GetRemoteRegistration(ctx, stored.ID); err != nil || current.Revision != updated.Revision {
		t.Fatalf("rejected enrollment changed the registration: %+v %v", current, err)
	}
}

func TestHostStartConvertsOlderRegistrationProtocolVersion(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	versions := map[string]string{"older": "1", "newer": "99"}
	for id, version := range versions {
		if _, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: id, Endpoint: "controller", ProtocolVersion: version, PerEnvPIDs: 1, PerEnvDiskBytes: 1,
			MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 3, ConnectorPhase: 1, EnrollmentRequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	controller := &Controller{store: database, externalConnectors: map[string]*externalConnectorRoute{}}
	for range 2 {
		if err := controller.StartExternalConnectors(ctx); err != nil {
			t.Fatal(err)
		}
	}
	older, err := database.GetRemoteRegistration(ctx, "older")
	if err != nil || older.ProtocolVersion != remoteconnector.ProtocolVersion || older.Revision != 2 {
		t.Fatalf("older registration = %+v %v", older, err)
	}
	if newer, err := database.GetRemoteRegistration(ctx, "newer"); err != nil || newer.ProtocolVersion != "99" || newer.Revision != 1 {
		t.Fatalf("newer registration = %+v %v", newer, err)
	}
}

func TestExactRegistrationReadSeparatesAbsentFromUnreadable(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
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
	database, err := openTestStore(path)
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

func TestRevokeIncompleteRegistrationKeepsOtherRegistration(t *testing.T) {
	for _, enrollment := range []int32{1, 3} {
		t.Run(fmt.Sprint(enrollment), func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			create := func(id string, state int32) store.RemoteRegistration {
				r, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: id, Endpoint: "controller", ProtocolVersion: "2", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 5, State: 1, ConnectionState: 1, EnrollmentState: state, ConnectorPhase: 1, EnrollmentRequestID: id})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			target, neighbor := create("failed", enrollment), create("neighbor", 2)
			server := &operatorServer{controller: &Controller{store: database}}
			call := func(action func(context.Context) (any, error)) (any, error) {
				return auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) { return action(ctx) })
			}
			result, err := call(func(ctx context.Context) (any, error) {
				return server.BeginRemoteRevocation(ctx, &hostv1.BeginRemoteRevocationRequest{RegistrationId: target.ID, ExpectedRevision: target.Revision, RequestId: "remove"})
			})
			if err != nil {
				t.Fatal(err)
			}
			begun := result.(*hostv1.RemoteRegistrationResponse).Registration
			_, err = call(func(ctx context.Context) (any, error) {
				return server.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RegistrationId: target.ID, ExpectedRevision: begun.Revision, RequestId: "remove"})
			})
			if err != nil {
				t.Fatal(err)
			}
			revoked, err := database.GetRemoteRegistration(ctx, target.ID)
			if err != nil || revoked.State != 3 {
				t.Fatalf("revoked=%+v err=%v", revoked, err)
			}
			current, err := database.GetRemoteRegistration(ctx, neighbor.ID)
			if err != nil || current.Revision != neighbor.Revision || current.State != 1 {
				t.Fatalf("neighbor changed: %+v %v", current, err)
			}
			if _, err := database.SetRemoteRegistrationState(ctx, revoked.ID, revoked.Revision, 1); err == nil {
				t.Fatal("revoked registration enabled")
			}
		})
	}
}
