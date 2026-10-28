//go:build darwin || linux

package remote

import (
	"context"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestProtocolRefusalProjectionFollowsAcceptedRegistrationStatus(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx := context.Background()
	for _, id := range []string{"registration-a", "registration-b"} {
		_, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: id, Endpoint: "controller", PerEnvPIDs: 128, PerEnvDiskBytes: 4 << 30, MaxEgressMode: 1, StopGraceSeconds: 10, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	owner := newTestOwner(&Owner{store: database})
	refusal := &hostv1.ConnectorStatus{Phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, FailureCode: "REMOTE_PROTOCOL_UNSUPPORTED", RemoteProtocol: &hostv1.RemoteProtocolStatus{HostProtocol: "4", MinProtocol: 5, MaxProtocol: 5, HostSide: "too_old"}}
	report := func(status *hostv1.ConnectorStatus, instance string) error {
		_, err := owner.ReportConnectorStatus(ctx, &connectorv1.ReportConnectorStatusRequest{RegistrationId: "registration-a", ConnectionInstance: instance, Status: status})
		return err
	}
	read := func(owner *Owner, id string) *hostv1.RemoteProtocolStatus {
		result, err := owner.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: id})
		if err != nil {
			t.Fatal(err)
		}
		if result.GetRegistration().GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED {
			t.Fatal("diagnosis changed registration admission")
		}
		return result.GetRegistration().GetConnectorStatus().GetRemoteProtocol()
	}
	if err := report(refusal, ""); err != nil {
		t.Fatal(err)
	}
	if got := read(owner, "registration-a"); got.GetHostSide() != "too_old" || got.GetMinProtocol() != 5 {
		t.Fatalf("refusal projection=%v", got)
	}
	if got := read(owner, "registration-b"); got != nil {
		t.Fatalf("other registration received refusal=%v", got)
	}
	if got := read(newTestOwner(&Owner{store: database}), "registration-a"); got != nil {
		t.Fatalf("Host restart invented current metadata=%v", got)
	}
	if err := report(&hostv1.ConnectorStatus{Phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, FailureCode: "CONNECTION_FAILED"}, ""); err != nil {
		t.Fatal(err)
	}
	if got := read(owner, "registration-a"); got != nil {
		t.Fatalf("later transport failure retained refusal=%v", got)
	}
	if err := report(refusal, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetRemoteConnection(ctx, "registration-a", "current", 2); err != nil {
		t.Fatal(err)
	}
	if err := report(refusal, "old"); err == nil {
		t.Fatal("stale connector report was accepted")
	}
	if err := report(&hostv1.ConnectorStatus{Phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED}, "current"); err != nil {
		t.Fatal(err)
	}
	if got := read(owner, "registration-a"); got != nil {
		t.Fatalf("successful connection retained refusal=%v", got)
	}
	if _, err := database.SetRemoteConnection(ctx, "registration-a", "current", 1); err != nil {
		t.Fatal(err)
	}
	if got := read(owner, "registration-a"); got != nil {
		t.Fatalf("old refusal returned after disconnect=%v", got)
	}
}
