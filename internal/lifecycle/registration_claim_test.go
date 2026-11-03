//go:build darwin || linux

package lifecycle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
)

func validTestInstallMaterial() registrationConfiguration {
	return registrationConfiguration{Version: 1, RegistrationID: "registration-1", RemoteConnection: remoteConnectionMaterial{GatewayEndpoint: "https://gateway.example.test", PolicyRevision: 1, PoolPolicy: remotePoolMaterial{PerEnvironmentLimits: resourcePolicy{PIDs: 128, DiskBytes: 4 << 30}, MaxEgressMode: "deny_all", StopGraceSeconds: 1}}}
}
func testClaimPlan(requestID, endpoint string) *registrationClaimPlan {
	return &registrationClaimPlan{Version: 2, RequestID: requestID, Descriptor: registrationClaimDescriptor{Version: 1, RegistrationID: "registration-1", ClaimURL: endpoint + registrationClaimPath, Ticket: "ticket", ExpiresAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}, DeviceID: "device", RootPublicKey: make([]byte, 33), RootKeyRevision: 1, CandidateCredential: []byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))}
}
func TestClaimDescriptorProtectsTicketDestination(t *testing.T) {
	descriptor := testClaimPlan("request", "https://api.example.test").Descriptor
	raw, _ := json.Marshal(descriptor)
	if _, err := decodeRegistrationClaimDescriptor(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"http://remote.example.test", "https://user@api.example.test", "http://localhost.example.test"} {
		descriptor.ClaimURL = endpoint + registrationClaimPath
		raw, _ = json.Marshal(descriptor)
		if _, err := decodeRegistrationClaimDescriptor(bytes.NewReader(raw)); err == nil {
			t.Fatalf("unsafe destination accepted: %s", endpoint)
		}
	}
}

type claimIdentityServer struct {
	hostv1.UnimplementedOperatorServiceServer
}

func (claimIdentityServer) GetDeviceIdentity(context.Context, *hostv1.GetDeviceIdentityRequest) (*hostv1.GetDeviceIdentityResponse, error) {
	return &hostv1.GetDeviceIdentityResponse{State: hostv1.DeviceIdentityState_DEVICE_IDENTITY_STATE_READY, Identity: &trustv1.DeviceIdentity{DeviceId: "device", RootPublicKey: make([]byte, 33), RootKeyRevision: 1}}, nil
}
func startClaimIdentity(t *testing.T, value paths) {
	t.Helper()
	if err := os.MkdirAll(value.applicationData, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	hostv1.RegisterOperatorServiceServer(server, claimIdentityServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
}
func TestClaimPersistsCandidateBeforeRequestAndReusesItAfterLostReply(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "claim-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	value := testPaths(root)
	startClaimIdentity(t, value)
	var accepted []byte
	calls := 0
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		saved, err := loadClaimPlan(value, "native-request")
		if err != nil {
			t.Errorf("request preceded durable candidate: %v", err)
			w.WriteHeader(500)
			return
		}
		if r.Header.Get("Authorization") != "RegistrationTicket ticket" {
			t.Error("claim authorization missing")
		}
		var body struct {
			Candidate      []byte `json:"candidate_credential"`
			RegistrationID string `json:"registration_id"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || !bytes.Equal(body.Candidate, saved.CandidateCredential) || body.RegistrationID != saved.Descriptor.RegistrationID {
			t.Error("claim target differs from durable plan")
		}
		if accepted == nil {
			accepted = append([]byte(nil), body.Candidate...)
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		if !bytes.Equal(accepted, body.Candidate) {
			t.Error("unknown result replaced candidate")
		}
		json.NewEncoder(w).Encode(validTestInstallMaterial())
	}))
	defer service.Close()
	descriptor := testClaimPlan("native-request", service.URL).Descriptor
	adapter := &productionLifecycleAdapter{value: value, requestID: "native-request", descriptor: &descriptor}
	if err := adapter.ensureRegistrationPlan(context.Background()); err == nil {
		t.Fatal("lost response unexpectedly succeeded")
	}
	resumed, err := productionRegistrationAdapterForResume(value, "native-request")
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.ensureRegistrationPlan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || resumed.material.RegistrationID != "registration-1" || !bytes.Equal(resumed.plan.CandidateCredential, accepted) {
		t.Fatal("recovery did not retain one candidate and target")
	}
	info, err := os.Stat(filepath.Join(value.applicationData, "install-operations", "native-request", "plan.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private plan: %v %v", info, err)
	}
}
func TestClaimWriteFailureDoesNotSendRequest(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "claim-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	value := testPaths(root)
	startClaimIdentity(t, value)
	if err := os.Symlink(t.TempDir(), filepath.Join(value.applicationData, "install-operations")); err != nil {
		t.Fatal(err)
	}
	calls := 0
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; io.Copy(io.Discard, r.Body) }))
	defer service.Close()
	descriptor := testClaimPlan("request", service.URL).Descriptor
	adapter := &productionLifecycleAdapter{value: value, requestID: "request", descriptor: &descriptor}
	if err := adapter.ensureRegistrationPlan(context.Background()); err == nil || calls != 0 {
		t.Fatalf("write failure sent %d requests: %v", calls, err)
	}
}
func TestClaimDoesNotForwardSecretsThroughRedirect(t *testing.T) {
	calls := 0
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer destination.Close()
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 307) }))
	defer service.Close()
	_, err := claimRegistration(context.Background(), testClaimPlan("request", service.URL))
	if err == nil || calls != 0 {
		t.Fatalf("claim redirected secret: %v calls=%d", err, calls)
	}
}
func TestClaimPlanRejectsChangedTarget(t *testing.T) {
	value := testPaths(t.TempDir())
	plan := testClaimPlan("request", "https://api.example.test")
	if err := persistClaimPlan(value, plan); err != nil {
		t.Fatal(err)
	}
	descriptor := plan.Descriptor
	descriptor.RegistrationID = "other"
	adapter := &productionLifecycleAdapter{value: value, requestID: "request", descriptor: &descriptor}
	if err := adapter.ensureRegistrationPlan(context.Background()); err == nil || !strings.Contains(err.Error(), "target changed") {
		t.Fatalf("changed target accepted: %v", err)
	}
}

func testServiceIdentity(t *testing.T) (int, int) {
	t.Helper()
	if os.Geteuid() == 0 {
		return 65534, 65534
	}
	groups, err := os.Getgroups()
	if err != nil {
		t.Fatal(err)
	}
	for _, gid := range groups {
		if gid != os.Getegid() {
			return os.Geteuid(), gid
		}
	}
	return os.Geteuid(), os.Getegid()
}
func assertFileOwner(t *testing.T, path string, uid, gid int) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if int(stat.Uid) != uid || int(stat.Gid) != gid {
		t.Fatalf("state owner=%d:%d want=%d:%d", stat.Uid, stat.Gid, uid, gid)
	}
}

func TestClaimPreservesAuthorizationFailureInsteadOfRetryingAsUnavailable(t *testing.T) {
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `{"error":"registration_revoked"}`)
	}))
	defer service.Close()
	_, err := claimRegistration(context.Background(), testClaimPlan("request", service.URL))
	_, code, _, retryable := lifecycleFailure("registration-claim", err)
	if code != "registration_revoked" || retryable {
		t.Fatalf("code=%s retryable=%v err=%v", code, retryable, err)
	}
}
func TestRecoveryDescriptorRequiresExactProductRoute(t *testing.T) {
	for _, route := range []string{"/v1/comma/workspaces/workspace/compute-nodes/agent-vmm/registrations/registration/recovery", "/v1/orgs/org/projects/project/compute-nodes/agent-vmm/registrations/registration/recovery"} {
		if !validRecoveryURL("https://api.example.test"+route, "registration") {
			t.Fatalf("supported route rejected: %s", route)
		}
		if validRecoveryURL("https://api.example.test"+route, "other") {
			t.Fatal("recovery accepted another registration")
		}
	}
	if validRecoveryURL("https://api.example.test/v1/compute/agent-vmm/registrations/registration/recovery", "registration") {
		t.Fatal("recovery accepted a route without the product owner")
	}
}

func TestRefreshedPendingTicketRetainsCandidateAndHost(t *testing.T) {
	value := testPaths(t.TempDir())
	var expected *registrationClaimPlan
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "RegistrationTicket fresh-ticket" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var request struct {
			Candidate []byte `json:"candidate_credential"`
			DeviceID  string `json:"device_id"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || !bytes.Equal(request.Candidate, expected.CandidateCredential) || request.DeviceID != expected.DeviceID {
			t.Error("ticket refresh replaced credential or Host identity")
			w.WriteHeader(400)
			return
		}
		json.NewEncoder(w).Encode(validTestInstallMaterial())
	}))
	defer service.Close()
	expected = testClaimPlan("request", service.URL)
	if err := persistClaimPlan(value, expected); err != nil {
		t.Fatal(err)
	}
	descriptor := expected.Descriptor
	descriptor.Ticket = "fresh-ticket"
	adapter := &productionLifecycleAdapter{value: value, requestID: "request", descriptor: &descriptor}
	if err := adapter.ensureRegistrationPlan(context.Background()); err != nil {
		t.Fatal(err)
	}
	persisted, err := loadClaimPlan(value, "request")
	if err != nil || persisted.Descriptor.Ticket != "fresh-ticket" || !bytes.Equal(persisted.CandidateCredential, expected.CandidateCredential) {
		t.Fatalf("persisted refresh changed candidate: %v", err)
	}
}
