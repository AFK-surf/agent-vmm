//go:build darwin && e2e

package e2e

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type productClient struct {
	operator hostv1.OperatorServiceClient

	mu       sync.Mutex
	sessions map[string]*productSession
}

type productSession struct {
	id         string
	connection *grpc.ClientConn
	client     hostv1.AgentRuntimeServiceClient
}

func newProductClient(operator hostv1.OperatorServiceClient) *productClient {
	return &productClient{operator: operator, sessions: make(map[string]*productSession)}
}

func (client *productClient) close() {
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, session := range client.sessions {
		_ = session.connection.Close()
	}
	client.sessions = make(map[string]*productSession)
}

func (client *productClient) forgetSession(environmentID string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if session := client.sessions[environmentID]; session != nil {
		_ = session.connection.Close()
		delete(client.sessions, environmentID)
	}
}

func (client *productClient) EnsureEnvironment(ctx context.Context, environmentID, requestID string, limits *hostv1.ResourceLimits, egress hostv1.EgressMode) (*hostv1.AgentEnvironment, error) {
	client.mu.Lock()
	if session := client.sessions[environmentID]; session != nil {
		client.mu.Unlock()
		response, err := session.client.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
		return response.GetEnvironment(), err
	}
	client.mu.Unlock()
	_, err := client.operator.EnsureLocalEnvironment(ctx, &hostv1.EnsureLocalEnvironmentRequest{RequestId: requestID, EnvironmentId: environmentID, DisplayName: environmentID})
	if err != nil {
		return nil, err
	}
	created, err := client.operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: requestID + "-session", EnvironmentId: environmentID})
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(created.GetSocketPath()); err != nil || info.Mode().Perm() != 0o600 {
		return nil, errors.New("agent session socket is not private")
	}
	connection, err := grpc.NewClient("passthrough:///agent", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", created.GetSocketPath())
	}))
	if err != nil {
		return nil, err
	}
	session := &productSession{id: created.GetSessionId(), connection: connection, client: hostv1.NewAgentRuntimeServiceClient(connection)}
	client.mu.Lock()
	client.sessions[environmentID] = session
	client.mu.Unlock()
	updated, err := client.operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{
		RequestId: requestID + "-policy", EnvironmentId: environmentID, Limits: limits, EgressMode: egress,
		ExpectedRevision: created.GetEnvironment().GetPolicyRevision(),
	})
	if err != nil {
		return nil, err
	}
	return updated.GetEnvironment(), nil
}

func optional[T any](value T) *T { return &value }

func (client *productClient) session(environmentID string) (*productSession, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	session := client.sessions[environmentID]
	if session == nil {
		return nil, errors.New("agent environment session does not exist")
	}
	return session, nil
}

func (client *productClient) DeleteEnvironment(ctx context.Context, environmentID, requestID string) error {
	plan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanEnvironmentRequest{EnvironmentId: environmentID})
	if err != nil {
		return err
	}
	session, _ := client.session(environmentID)
	var volumes []string
	if session != nil {
		listed, listErr := session.client.ListVolumes(ctx, &hostv1.ListVolumesRequest{})
		if listErr == nil {
			for _, volume := range listed.GetVolumes() {
				volumes = append(volumes, volume.GetId())
			}
		}
	}
	if _, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: requestID, Plan: plan.GetPlan(), ConfirmedVolumeIds: volumes}); err != nil {
		return err
	}
	client.mu.Lock()
	if session := client.sessions[environmentID]; session != nil {
		_ = session.connection.Close()
		delete(client.sessions, environmentID)
	}
	client.mu.Unlock()
	return nil
}

func (client *productClient) PullImage(ctx context.Context, environmentID string, request *hostv1.PullImageRequest) (grpc.ServerStreamingClient[hostv1.PullImageResponse], error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.PullImage(ctx, request)
}

func (client *productClient) CreateVolume(ctx context.Context, environmentID string, request *hostv1.CreateVolumeRequest) (*hostv1.VolumeResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.CreateVolume(ctx, request)
}

func (client *productClient) DeleteVolume(ctx context.Context, environmentID string, request *hostv1.DeleteVolumeRequest) error {
	session, err := client.session(environmentID)
	if err != nil {
		return err
	}
	_, err = session.client.DeleteVolume(ctx, request)
	return err
}

func (client *productClient) CreateContainer(ctx context.Context, environmentID string, request *hostv1.CreateContainerRequest) (*hostv1.ContainerResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.CreateContainer(ctx, request)
}

func (client *productClient) StartContainer(ctx context.Context, environmentID string, request *hostv1.ContainerMutationRequest) (*hostv1.ContainerResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.StartContainer(ctx, request)
}

func (client *productClient) StopContainer(ctx context.Context, environmentID string, request *hostv1.StopContainerRequest) (*hostv1.ContainerResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.StopContainer(ctx, request)
}

func (client *productClient) WaitContainer(ctx context.Context, environmentID string, request *hostv1.ContainerRequest) (*hostv1.WaitContainerResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.WaitContainer(ctx, request)
}

func (client *productClient) DeleteContainer(ctx context.Context, environmentID string, request *hostv1.ContainerMutationRequest) error {
	session, err := client.session(environmentID)
	if err != nil {
		return err
	}
	_, err = session.client.DeleteContainer(ctx, request)
	return err
}

func (client *productClient) GetContainer(ctx context.Context, environmentID string, request *hostv1.ContainerRequest) (*hostv1.ContainerResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.GetContainer(ctx, request)
}

func (client *productClient) ListContainers(ctx context.Context, environmentID string) (*hostv1.ListContainersResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.ListContainers(ctx, &hostv1.ListContainersRequest{})
}

func (client *productClient) Logs(ctx context.Context, environmentID string, request *hostv1.LogsRequest) (grpc.ServerStreamingClient[hostv1.LogRecord], error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.Logs(ctx, request)
}

func (client *productClient) Exec(ctx context.Context, environmentID string) (grpc.BidiStreamingClient[hostv1.ExecRequest, hostv1.ExecResponse], error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.Exec(ctx)
}

func (client *productClient) ForwardTCP(ctx context.Context, environmentID string) (grpc.BidiStreamingClient[hostv1.ForwardTCPRequest, hostv1.ForwardTCPResponse], error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.ForwardTCP(ctx)
}

func (client *productClient) BuildImage(ctx context.Context, environmentID string) (grpc.BidiStreamingClient[hostv1.BuildImageRequest, hostv1.BuildImageResponse], error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.BuildImage(ctx)
}

func (client *productClient) Snapshot(ctx context.Context) (*hostv1.GetSystemSnapshotResponse, error) {
	return client.operator.GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
}

func (client *productClient) ApplyNetworkLink(ctx context.Context, request *hostv1.ApplyNetworkLinkRequest) error {
	_, err := client.operator.ApplyNetworkLink(ctx, request)
	return err
}

func (client *productClient) DeleteNetworkLink(ctx context.Context, request *hostv1.DeleteNetworkLinkRequest) error {
	_, err := client.operator.DeleteNetworkLink(ctx, request)
	return err
}
