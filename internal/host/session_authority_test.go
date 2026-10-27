//go:build darwin || linux

package host

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type integrationRuntimeProbe struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	controller *Controller
	starts     atomic.Int32
}

func (probe *integrationRuntimeProbe) StartContainer(context.Context, *hostv1.StartContainerRequest) (*hostv1.StartContainerResponse, error) {
	probe.starts.Add(1)
	return &hostv1.StartContainerResponse{}, nil
}
func (probe *integrationRuntimeProbe) ImportImage(stream grpc.ClientStreamingServer[hostv1.ImportImageRequest, hostv1.ImportImageResponse]) error {
	return (&agentServer{controller: probe.controller}).ImportImage(stream)
}

func (probe *integrationRuntimeProbe) ReleaseSession(ctx context.Context, request *hostv1.ReleaseSessionRequest) (*hostv1.ReleaseSessionResponse, error) {
	return (&agentServer{controller: probe.controller}).ReleaseSession(ctx, request)
}

func integrationAuthorityFixture(t *testing.T) (*Controller, store.AgentIntegration, *integrationProbeFactory) {
	t.Helper()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	if _, err := database.CreateEnvironment(t.Context(), store.Environment{ID: "environment", NamespaceID: "namespace", DisplayName: "Service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	integration, err := database.CreateAgentIntegration(t.Context(), store.AgentIntegration{ID: "integration", EnvironmentID: "environment", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", ConfigFingerprint: "first", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database}
	factory := &integrationProbeFactory{controller: controller, entered: make(chan string, 8), probes: make(chan *integrationRuntimeProbe, 8)}
	config := testSessionConfig(t, controller)
	config.Runtime = factory
	config.StreamInterceptors = append([]grpc.StreamServerInterceptor{func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		factory.entered <- info.FullMethod
		return handler(service, stream)
	}}, config.StreamInterceptors...)
	controller.sessions = session.New(config)
	t.Cleanup(controller.sessions.Close)
	return newTestController(controller), integration, factory
}

type integrationProbeFactory struct {
	controller *Controller
	entered    chan string
	probes     chan *integrationRuntimeProbe
}

func (factory *integrationProbeFactory) RuntimeService(context.Context) hostv1.AgentRuntimeServiceServer {
	probe := &integrationRuntimeProbe{controller: factory.controller}
	factory.probes <- probe
	return probe
}
func integrationSessionProbe(t *testing.T, controller *Controller, integration store.AgentIntegration, factory *integrationProbeFactory) (hostv1.AgentRuntimeServiceClient, *integrationRuntimeProbe, <-chan string, store.Session) {
	t.Helper()
	record := createTestSession(t, controller, integration.EnvironmentID, integration.ID, integration.Revision, auth.Identity{})
	client, _ := dialTestSession(t, record)
	return client, <-factory.probes, factory.entered, record
}

func TestNewIntegrationIntentCancelsIdleImportAndOldCommitCannotReopenAuthority(t *testing.T) {
	controller, integration, factory := integrationAuthorityFixture(t)
	operator := &operatorServer{controller: controller}
	install := &hostv1.CommitAgentIntegrationRequest{RequestId: "install", IntegrationId: integration.ID, ExpectedRevision: integration.Revision, ConfigFingerprint: "first"}
	first, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CommitAgentIntegrationResponse, error) {
		return operator.CommitAgentIntegration(ctx, install)
	})
	if err != nil {
		t.Fatal(err)
	}
	integration, err = controller.store.GetAgentIntegration(t.Context(), integration.ID)
	if err != nil {
		t.Fatal(err)
	}
	client, oldProbe, entered, oldSession := integrationSessionProbe(t, controller, integration, factory)
	stream, err := client.ImportImage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("idle import was not admitted")
	}
	stop := &hostv1.CommitAgentIntegrationRequest{RequestId: "stop", IntegrationId: integration.ID, ExpectedRevision: integration.Revision, ConfigFingerprint: "first"}
	stopped, err := callAsOperator(t, func(operatorContext context.Context) (*hostv1.CommitAgentIntegrationResponse, error) {
		ctx, cancel := context.WithTimeout(operatorContext, time.Second)
		defer cancel()
		return operator.CommitAgentIntegration(ctx, stop)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("idle old import survived new intent")
	}
	if _, err := client.StartContainer(t.Context(), &hostv1.StartContainerRequest{}); err == nil || oldProbe.starts.Load() != 0 {
		t.Fatalf("old authority survived revocation: %v", err)
	}
	stored, err := controller.store.GetSession(t.Context(), oldSession.ID)
	if err != nil || stored.RevokedAt == nil {
		t.Fatalf("old session revocation=%+v %v", stored, err)
	}
	integration, err = controller.store.GetAgentIntegration(t.Context(), integration.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, _, _, _ := integrationSessionProbe(t, controller, integration, factory)
	replayed, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CommitAgentIntegrationResponse, error) {
		return operator.CommitAgentIntegration(ctx, install)
	})
	if err != nil || replayed.GetIntegration().GetRevision() != first.GetIntegration().GetRevision() || replayed.GetIntegration().GetRevision() == stopped.GetIntegration().GetRevision() {
		t.Fatalf("original receipt=%v %v", replayed, err)
	}
	if _, err := current.StartContainer(t.Context(), &hostv1.StartContainerRequest{}); err != nil {
		t.Fatalf("replay cancelled current authority: %v", err)
	}
	_, err = callAsOperator(t, func(ctx context.Context) (*hostv1.CreateAgentSessionResponse, error) {
		return operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: "stale-open", IntegrationId: integration.ID, ExpectedIntegrationRevision: optional(first.GetIntegration().GetRevision())})
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale session admission=%v", err)
	}
}

func TestOperatorStopSavesIntentWithoutGuestOrSessionAndRejectsOldClaim(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-first-start", true: "running-intent"}[existing], func(t *testing.T) {
			controller, integration, _ := integrationAuthorityFixture(t)
			controller.manager = &vmm.Manager{}
			if existing {
				if err := controller.store.SetManagedContainerRunning(t.Context(), integration.EnvironmentID, "component", "generation", true); err != nil {
					t.Fatal(err)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})))
			hostv1.RegisterOperatorServiceServer(server, &operatorServer{controller: controller})
			go server.Serve(listener)
			t.Cleanup(func() { server.Stop(); listener.Close() })
			connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { connection.Close() })
			client := hostv1.NewOperatorServiceClient(connection)
			request := &hostv1.OperatorServiceStopContainerRequest{RequestId: "stop", EnvironmentId: integration.EnvironmentID, ContainerId: "component", IntegrationId: optional(integration.ID), ExpectedIntegrationRevision: optional(integration.Revision)}
			_, err = client.StopContainer(t.Context(), request)
			pending := false
			for _, detail := range status.Convert(err).Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Reason == "SERVICE_STOP_PENDING" && info.Metadata["desired_running"] == "false" && info.Metadata["actual_state"] == "unknown" {
					pending = true
				}
			}
			if status.Code(err) != codes.Unavailable || !pending {
				t.Fatalf("accepted stop projection: %v", err)
			}
			intent, err := controller.store.GetManagedContainer(t.Context(), integration.EnvironmentID, "component")
			if err != nil || intent.DesiredRunning || (existing && intent.ContainerGenerationID != "generation") {
				t.Fatalf("saved stop intent=%+v %v", intent, err)
			}
			sessions, listErr := controller.store.ListSessions(t.Context(), "", true)
			if listErr != nil {
				t.Fatal(listErr)
			}
			if len(sessions) != 0 || controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING || controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_STARTING {
				t.Fatal("Stop created session or started VM")
			}
			if _, err := client.CommitAgentIntegration(t.Context(), &hostv1.CommitAgentIntegrationRequest{RequestId: "new-start", IntegrationId: integration.ID, ExpectedRevision: integration.Revision, ConfigFingerprint: "first"}); err != nil {
				t.Fatal(err)
			}
			if err := controller.store.SetManagedContainerRunning(t.Context(), integration.EnvironmentID, "component", "new-generation", true); err != nil {
				t.Fatal(err)
			}
			_, err = client.StopContainer(t.Context(), request)
			if status.Code(err) != codes.Aborted {
				t.Fatalf("old Stop claim=%v", err)
			}
			intent, err = controller.store.GetManagedContainer(t.Context(), integration.EnvironmentID, "component")
			if err != nil || !intent.DesiredRunning || intent.ContainerGenerationID != "new-generation" {
				t.Fatalf("old Stop changed new intent=%+v %v", intent, err)
			}
		})
	}
}
