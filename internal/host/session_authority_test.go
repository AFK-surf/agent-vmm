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
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type integrationRuntimeProbe struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	controller *Controller
	starts     atomic.Int32
}

func (probe *integrationRuntimeProbe) StartContainer(context.Context, *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	probe.starts.Add(1)
	return &hostv1.ContainerResponse{}, nil
}
func (probe *integrationRuntimeProbe) ImportImage(stream grpc.ClientStreamingServer[hostv1.ImportImageRequest, hostv1.Image]) error {
	return (&agentServer{controller: probe.controller}).ImportImage(stream)
}

func (probe *integrationRuntimeProbe) ReleaseSession(ctx context.Context, request *hostv1.ReleaseSessionRequest) (*emptypb.Empty, error) {
	return (&agentServer{controller: probe.controller}).ReleaseSession(ctx, request)
}

func integrationAuthorityFixture(t *testing.T) (*Controller, store.AgentIntegration) {
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
	return &Controller{store: database, sessions: make(map[string]*sessionServer)}, integration
}

func integrationSessionProbe(t *testing.T, controller *Controller, integration store.AgentIntegration, id string) (hostv1.AgentRuntimeServiceClient, *integrationRuntimeProbe, <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tracked := &sessionServer{listener: listener, integrationID: integration.ID, integrationRevision: integration.Revision, stopDone: make(chan struct{})}
	entered := make(chan string, 8)
	identity := auth.Identity{Role: auth.Agent, SessionID: id, EnvironmentID: integration.EnvironmentID, NamespaceID: "namespace"}
	server := grpc.NewServer(grpc.WaitForHandlers(true), grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(identity), func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		entered <- info.FullMethod
		return handler(ctx, request)
	}, sessionActivityUnary(controller, id)), grpc.ChainStreamInterceptor(auth.StreamInterceptor(identity), sessionActivityStream(controller, id), func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		entered <- info.FullMethod
		return handler(service, stream)
	}))
	tracked.server = server
	probe := &integrationRuntimeProbe{controller: controller}
	hostv1.RegisterAgentRuntimeServiceServer(server, probe)
	if err := controller.store.CreateAuthorizedSession(t.Context(), store.Session{ID: id, IntegrationID: integration.ID, EnvironmentID: integration.EnvironmentID, SocketPath: listener.Addr().String(), Origin: 1, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}, integration.Revision); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	controller.sessions[id] = tracked
	controller.mu.Unlock()
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return hostv1.NewAgentRuntimeServiceClient(connection), probe, entered
}

func TestNewIntegrationIntentCancelsIdleImportAndOldCommitCannotReopenAuthority(t *testing.T) {
	controller, integration := integrationAuthorityFixture(t)
	operator := &operatorServer{controller: controller}
	install := &hostv1.CommitAgentIntegrationRequest{RequestId: "install", IntegrationId: integration.ID, ExpectedRevision: integration.Revision, ConfigFingerprint: "first"}
	first, err := callAsOperator(t, func(ctx context.Context) (*hostv1.AgentIntegrationResponse, error) {
		return operator.CommitAgentIntegration(ctx, install)
	})
	if err != nil {
		t.Fatal(err)
	}
	integration, err = controller.store.GetAgentIntegration(t.Context(), integration.ID)
	if err != nil {
		t.Fatal(err)
	}
	client, _, entered := integrationSessionProbe(t, controller, integration, "old")
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
	stopped, err := callAsOperator(t, func(operatorContext context.Context) (*hostv1.AgentIntegrationResponse, error) {
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
	if err := controller.authorizeSession(t.Context(), "old"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("old authority=%v", err)
	}
	integration, err = controller.store.GetAgentIntegration(t.Context(), integration.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = integrationSessionProbe(t, controller, integration, "current")
	replayed, err := callAsOperator(t, func(ctx context.Context) (*hostv1.AgentIntegrationResponse, error) {
		return operator.CommitAgentIntegration(ctx, install)
	})
	if err != nil || replayed.GetIntegration().GetRevision() != first.GetIntegration().GetRevision() || replayed.GetIntegration().GetRevision() == stopped.GetIntegration().GetRevision() {
		t.Fatalf("original receipt=%v %v", replayed, err)
	}
	if err := controller.authorizeSession(t.Context(), "current"); err != nil {
		t.Fatalf("replay cancelled current authority: %v", err)
	}
	_, err = callAsOperator(t, func(ctx context.Context) (*hostv1.CreateAgentSessionResponse, error) {
		return operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: "stale-open", IntegrationId: integration.ID, ExpectedIntegrationRevision: optional(first.GetIntegration().GetRevision())})
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale session admission=%v", err)
	}
}

func TestIntegrationMutationRechecksRevisionAfterAdmissionLock(t *testing.T) {
	controller, integration := integrationAuthorityFixture(t)
	client, probe, entered := integrationSessionProbe(t, controller, integration, "old")
	unlock := controller.lockIntegration(integration.ID)
	result := make(chan error, 1)
	go func() {
		_, err := client.StartContainer(t.Context(), &hostv1.ContainerMutationRequest{RequestId: "not-yet-started", ContainerId: "component", ExpectedGenerationId: "generation"})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		unlock()
		t.Fatal("old RPC did not reach admission")
	}
	_, err := controller.store.CommitAgentIntegration(t.Context(), "new", integration.ID, integration.Revision, "new", 2)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if status.Code(err) != codes.PermissionDenied || probe.starts.Load() != 0 {
			t.Fatalf("stale mutation=%v starts=%d", err, probe.starts.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("stale RPC did not finish")
	}
	if _, err := client.ReleaseSession(t.Context(), &hostv1.ReleaseSessionRequest{RequestId: "release-own"}); err != nil {
		t.Fatalf("stale session could not release itself: %v", err)
	}
	until := time.Now().Add(time.Second)
	for {
		controller.mu.Lock()
		_, active := controller.sessions["old"]
		controller.mu.Unlock()
		if !active {
			break
		}
		if time.Now().After(until) {
			t.Fatal("released stale session remained active")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOperatorStopSavesIntentWithoutGuestOrSessionAndRejectsOldClaim(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-first-start", true: "running-intent"}[existing], func(t *testing.T) {
			controller, integration := integrationAuthorityFixture(t)
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
			request := &hostv1.OperatorStopContainerRequest{RequestId: "stop", EnvironmentId: integration.EnvironmentID, ContainerId: "component", IntegrationId: optional(integration.ID), ExpectedIntegrationRevision: optional(integration.Revision)}
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
			if len(controller.sessions) != 0 || controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING || controller.manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_STARTING {
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
