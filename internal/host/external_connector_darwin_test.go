//go:build darwin

package host

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

func TestExternalConnectorSupervisorStartsAllAndStopsExactRegistration(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tlsServer := httptest.NewTLSServer(nil)
	defer tlsServer.Close()
	certificate, err := x509.ParseCertificate(tlsServer.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	trustBundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	registration := store.RemoteRegistration{
		ID: "registration-a", Endpoint: "controller.example.test:443", DisplayName: "a", ProtocolVersion: remoteconnector.ProtocolVersion,
		TrustBundle: trustBundle, PerEnvPIDs: 128, PerEnvDiskBytes: 2 << 30,
		MaxEgressMode:    int32(hostv1.EgressMode_EGRESS_MODE_DENY_ALL),
		StopGraceSeconds: 2,
		State:            int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED),
		ConnectionState:  int32(hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED),
		EnrollmentState:  int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY),
		ConnectorPhase:   int32(hostv1.ConnectorPhase_CONNECTOR_PHASE_STOPPED),
	}
	if _, err := database.CreateRemoteRegistration(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	registration.ID, registration.DisplayName = "registration-b", "b"
	if _, err := database.CreateRemoteRegistration(context.Background(), registration); err != nil {
		t.Fatal(err)
	}
	started, stopped := make(chan remoteconnector.Config, 2), make(chan string, 2)
	controller := &Controller{
		store: database, externalConnectors: make(map[string]*externalConnectorRoute),
		remoteComputeSocket: "/sockets/remote-compute.sock", trustSocket: "/sockets/trust.sock",
		trustCapabilityFile: "/capabilities/trust.cap", dataWorkerSocket: "/sockets/data-worker.sock",
		serviceTunnelSocket: "/services/tunnel.sock", meshControlSocket: "/mesh/control.sock",
		remoteCredentials: remoteconnector.NewCredentialStore(filepath.Join(t.TempDir(), "credentials")),
		externalConnectorRunner: func(ctx context.Context, config remoteconnector.Config) error {
			started <- config
			<-ctx.Done()
			stopped <- config.RegistrationID
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := controller.StartExternalConnectors(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for len(seen) != 2 {
		select {
		case config := <-started:
			seen[config.RegistrationID] = true
			if config.RemoteComputeSocket != "/sockets/remote-compute.sock" || config.TrustSocket != "/sockets/trust.sock" ||
				config.TrustCapabilityFile != "/capabilities/trust.cap" || config.DataWorkerSocket != "/sockets/data-worker.sock" ||
				config.ServiceTunnelSocket != "/services/tunnel.sock" || config.MeshControlSocket != "/mesh/control.sock" {
				t.Fatalf("connector socket authority drifted: %+v", config)
			}
		case <-time.After(time.Second):
			t.Fatalf("connectors started=%v", seen)
		}
	}
	controller.stopExternalConnector("registration-a")
	select {
	case id := <-stopped:
		if id != "registration-a" {
			t.Fatalf("stopped %q instead of registration-a", id)
		}
	case <-time.After(time.Second):
		t.Fatal("exact connector did not stop")
	}
	controller.externalConnectorMu.Lock()
	_, secondRunning := controller.externalConnectors["registration-b"]
	controller.externalConnectorMu.Unlock()
	if !secondRunning {
		t.Fatal("stopping registration-a stopped registration-b")
	}
	select {
	case id := <-stopped:
		t.Fatalf("unrelated connector stopped: %s", id)
	case <-time.After(50 * time.Millisecond):
	}
	controller.stopAllExternalConnectors()
	select {
	case id := <-stopped:
		if id != "registration-b" {
			t.Fatalf("VM stop stopped unexpected connector: %s", id)
		}
	case <-time.After(time.Second):
		t.Fatal("VM stop did not stop the remaining connector")
	}
	if err := controller.ReconcileExternalConnectors(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := map[string]bool{}
	for len(restarted) != 2 {
		select {
		case config := <-started:
			restarted[config.RegistrationID] = true
		case <-time.After(time.Second):
			t.Fatalf("connectors did not restart after VM start: %v", restarted)
		}
	}
	cancel()
	controller.stopAllExternalConnectors()
}
