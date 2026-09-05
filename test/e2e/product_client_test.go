//go:build darwin && e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type registryArchive struct {
	path   string
	digest string
	hash   []byte
	size   uint64
}

type productClient struct {
	archiveMu        sync.Mutex
	registryArchives map[string]registryArchive
	archiveDirectory string
	archiveURL       string
	operator         hostv1.OperatorServiceClient

	mu       sync.Mutex
	sessions map[string]*productSession
}

type productSession struct {
	id            string
	integrationID string
	connection    *grpc.ClientConn
	client        hostv1.AgentRuntimeServiceClient
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

func (client *productClient) refreshSessions(ctx context.Context) error {
	client.mu.Lock()
	existing := make(map[string]*productSession, len(client.sessions))
	for environmentID, session := range client.sessions {
		existing[environmentID] = session
	}
	client.mu.Unlock()
	for environmentID, session := range existing {
		created, err := client.operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: fmt.Sprintf("refresh-%s-%d", environmentID, time.Now().UnixNano()), IntegrationId: session.integrationID})
		if err != nil {
			return err
		}
		connection, err := grpc.NewClient("passthrough:///agent", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", created.GetSocketPath())
		}))
		if err != nil {
			return err
		}
		replacement := &productSession{id: created.GetSessionId(), integrationID: session.integrationID, connection: connection, client: hostv1.NewAgentRuntimeServiceClient(connection)}
		client.mu.Lock()
		if client.sessions[environmentID] == session {
			client.sessions[environmentID] = replacement
			client.mu.Unlock()
			_ = session.connection.Close()
		} else {
			client.mu.Unlock()
			_ = replacement.connection.Close()
		}
	}
	return nil
}

func (client *productClient) EnsureEnvironment(ctx context.Context, environmentID, requestID string, limits *hostv1.ResourceLimits, egress hostv1.EgressMode) (*hostv1.AgentEnvironment, error) {
	client.mu.Lock()
	if session := client.sessions[environmentID]; session != nil {
		client.mu.Unlock()
		response, err := session.client.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
		return response.GetEnvironment(), err
	}
	client.mu.Unlock()
	prepared, err := client.operator.PrepareAgentIntegration(ctx, &hostv1.PrepareAgentIntegrationRequest{
		RequestId: requestID + "-integration", AdapterId: "e2e", DisplayName: environmentID,
		Scope: hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL, ConfigTarget: "e2e://" + environmentID,
		EnvironmentId: optional(environmentID),
	})
	if err != nil {
		return nil, err
	}
	integration, err := client.operator.CommitAgentIntegration(ctx, &hostv1.CommitAgentIntegrationRequest{
		RequestId: requestID + "-integration-commit", IntegrationId: prepared.GetIntegration().GetId(),
		ExpectedRevision: prepared.GetIntegration().GetRevision(), ConfigFingerprint: "e2e",
	})
	if err != nil {
		return nil, err
	}
	created, err := client.operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: requestID + "-session", IntegrationId: integration.GetIntegration().GetId()})
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
	session := &productSession{id: created.GetSessionId(), integrationID: integration.GetIntegration().GetId(), connection: connection, client: hostv1.NewAgentRuntimeServiceClient(connection)}
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
	client.forgetSession(environmentID)
	if err := client.RemoveEnvironmentAccess(ctx, environmentID, requestID); err != nil {
		return err
	}
	plan, err := client.operator.PlanDiscardEnvironment(ctx, &hostv1.PlanEnvironmentRequest{EnvironmentId: environmentID})
	if err != nil {
		return err
	}
	response, err := client.operator.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: requestID, Plan: plan.GetPlan(), ConfirmedVolumeIds: volumes})
	if err != nil {
		return err
	}
	operation := response.GetOperation()
	switch operation.GetState() {
	case hostv1.OperationState_OPERATION_STATE_SUCCEEDED:
		return nil
	case hostv1.OperationState_OPERATION_STATE_FAILED:
		return fmt.Errorf("cleanup failed: %s: %s", operation.GetError().GetReason(), operation.GetError().GetMessage())
	default:
		return fmt.Errorf("cleanup operation did not finish: %s", operation.GetState())
	}
}

func (client *productClient) RemoveEnvironmentAccess(ctx context.Context, environmentID, requestID string) error {
	listed, err := client.operator.ListAgentIntegrations(ctx, &hostv1.ListAgentIntegrationsRequest{})
	if err != nil {
		return err
	}
	for _, integration := range listed.GetIntegrations() {
		if integration.GetEnvironmentId() != environmentID {
			continue
		}
		removing, err := client.operator.BeginAgentIntegrationRemoval(ctx, &hostv1.BeginAgentIntegrationRemovalRequest{RequestId: requestID + "-remove-access-" + integration.GetId(), IntegrationId: integration.GetId(), ExpectedRevision: integration.GetRevision()})
		if err != nil {
			return err
		}
		if _, err := client.operator.CompleteAgentIntegrationRemoval(ctx, &hostv1.CompleteAgentIntegrationRemovalRequest{RequestId: requestID + "-complete-remove-access-" + integration.GetId(), IntegrationId: integration.GetId(), ExpectedRevision: removing.GetIntegration().GetRevision()}); err != nil {
			return err
		}
	}
	return nil
}

func (client *productClient) ImportImage(ctx context.Context, environmentID string, header *hostv1.ImportImageHeader, archive io.Reader) (*hostv1.Image, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(client.archiveDirectory, "import-*.tar")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	_, copyErr := io.Copy(file, archive)
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return nil, err
	}
	header = proto.Clone(header).(*hostv1.ImportImageHeader)
	header.ArchiveUrl = client.archiveURL + "/" + filepath.Base(file.Name())
	// Preserve the caller's commitment so corruption and size tests still reach
	// the Host/Guest validation boundary.
	stream, err := session.client.ImportImage(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&hostv1.ImportImageRequest{Payload: &hostv1.ImportImageRequest_Header{Header: header}}); err != nil {
		if errors.Is(err, io.EOF) {
			return stream.CloseAndRecv()
		}
		return nil, err
	}
	return stream.CloseAndRecv()
}

func (client *productClient) ListImages(ctx context.Context, environmentID string) (*hostv1.ListImagesResponse, error) {
	session, err := client.session(environmentID)
	if err != nil {
		return nil, err
	}
	return session.client.ListImages(ctx, &hostv1.ListImagesRequest{})
}

func (client *productClient) DeleteImage(ctx context.Context, environmentID string, request *hostv1.DeleteImageRequest) error {
	session, err := client.session(environmentID)
	if err != nil {
		return err
	}
	_, err = session.client.DeleteImage(ctx, request)
	return err
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
	if request.GetExpectedGenerationId() == "" {
		current, err := session.client.GetContainer(ctx, &hostv1.ContainerRequest{ContainerId: request.GetContainerId()})
		if err != nil {
			return err
		}
		request = proto.Clone(request).(*hostv1.ContainerMutationRequest)
		request.ExpectedGenerationId = current.GetContainer().GetGenerationId()
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
