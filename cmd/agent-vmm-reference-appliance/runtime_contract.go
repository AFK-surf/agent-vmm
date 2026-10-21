package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const referenceImage = "agent-vmm.local/reference-workload:v1"

func (controller referenceController) runReferenceContract(ctx context.Context, cursor *referenceCommandCursor, allocations map[string]*remotev1.Allocation) error {
	durable, durableOwner, err := controller.runtimeClient(ctx, "durable-service")
	if err != nil {
		return err
	}
	defer durableOwner.Close()
	task, taskOwner, err := controller.runtimeClient(ctx, "ephemeral-task")
	if err != nil {
		return err
	}
	taskClosed := false
	defer func() {
		if !taskClosed {
			_ = taskOwner.Close()
		}
	}()
	archive, manifestDigest, err := buildReferenceOCI()
	if err != nil {
		return err
	}
	for _, client := range []hostv1.AgentRuntimeServiceClient{durable, task} {
		if err := importReferenceImage(ctx, client, archive, manifestDigest); err != nil {
			return err
		}
	}
	if _, err := durable.CreateVolume(ctx, &hostv1.CreateVolumeRequest{RequestId: "reference-state", VolumeId: "state"}); err != nil {
		return err
	}
	mount := []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "state"}, Destination: "/data"}}
	service, err := createReferenceContainer(ctx, durable, "service", "service", mount)
	if err != nil {
		return err
	}
	if _, err := durable.StartContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: "start-service", ContainerId: service.GetId(), ExpectedGenerationId: service.GetGenerationId()}); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	sidecar, err := createReferenceContainer(ctx, durable, "sidecar", "sidecar", mount)
	if err != nil {
		return err
	}
	if _, err := durable.StartContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: "start-sidecar", ContainerId: sidecar.GetId(), ExpectedGenerationId: sidecar.GetGenerationId()}); err != nil {
		return err
	}
	cycle := fmt.Sprint(cursor.session.Hello().GetConnectionEpoch())
	exported, err := durable.CreateServiceExport(ctx, &hostv1.CreateServiceExportRequest{RequestId: "reference-export-" + cycle, ContainerId: service.GetId(), ContainerInstanceId: service.GetInstanceId(), Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP})
	if err != nil {
		return err
	}
	source, err := createReferenceContainer(ctx, task, "route-client", "task", nil)
	if err != nil {
		return err
	}
	environment, err := task.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
	if err != nil {
		return err
	}
	capability, err := controller.routeCapability(exported, allocations["ephemeral-task"], environment.GetEnvironment().GetPolicyRevision())
	if err != nil {
		return err
	}
	imported, err := task.CreateServiceImport(ctx, &hostv1.CreateServiceImportRequest{RequestId: "reference-import-" + cycle, SourceContainerId: source.GetId(), SourceContainerInstanceId: source.GetInstanceId(),
		DestinationDeviceId: capability.GetDestinationDeviceId(), DestinationExportId: exported.GetExportId(), VirtualServiceName: "reference-service", Capability: capability})
	if err != nil {
		return err
	}
	if _, err := task.StartContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: "start-route-client", ContainerId: source.GetId(), ExpectedGenerationId: source.GetGenerationId()}); err != nil {
		return err
	}
	wait, err := task.WaitContainer(ctx, &hostv1.ContainerRequest{ContainerId: source.GetId()})
	if err != nil || wait.GetExitCode() != 0 {
		return errors.Join(errors.New("reference service route traffic failed"), err)
	}
	if _, err := task.RevokeServiceImport(ctx, &hostv1.RevokeServiceImportRequest{RequestId: "reference-revoke-import-" + cycle, ImportId: imported.GetImportId(), ExpectedRevision: imported.GetRevision()}); err != nil {
		return err
	}
	if _, err := durable.RevokeServiceExport(ctx, &hostv1.RevokeOwnServiceExportRequest{RequestId: "reference-revoke-export-" + cycle, ExportId: exported.GetExportId(), ExpectedRevision: exported.GetRevision()}); err != nil {
		return err
	}
	if err := taskOwner.Close(); err != nil {
		return err
	}
	taskClosed = true
	if err := cleanupReferenceTaskAllocation(ctx, cursor, allocations["ephemeral-task"]); err != nil {
		return err
	}
	delete(allocations, "ephemeral-task")
	return nil
}

func cleanupReferenceTaskAllocation(ctx context.Context, cursor *referenceCommandCursor, allocation *remotev1.Allocation) error {
	if allocation == nil {
		return errors.New("ephemeral task allocation is unavailable")
	}
	if _, err := dispatchReferenceCommand(ctx, cursor, "release-task-allocation", &remotev1.ControllerCommand{TargetRevision: allocation.GetRevision(), Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: allocation.GetAllocationId(), ExpectedRevision: allocation.GetRevision(), ExpectedGeneration: allocation.GetGeneration()}}}); err != nil {
		return err
	}
	planned, err := dispatchReferenceCommand(ctx, cursor, "plan-task-cleanup", &remotev1.ControllerCommand{Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: allocation.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD}}})
	if err != nil {
		return err
	}
	plan := planned.GetCleanupPlan()
	_, err = dispatchReferenceCommand(ctx, cursor, "execute-task-cleanup", &remotev1.ControllerCommand{TargetRevision: plan.GetRevision(), Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: allocation.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: plan.GetRevision(), ConfirmedVolumeIds: plan.GetVolumeIds()}}})
	return err
}

func (controller referenceController) runtimeClient(ctx context.Context, allocationID string) (hostv1.AgentRuntimeServiceClient, *grpc.ClientConn, error) {
	connection, err := controller.sessions.take(ctx, allocationID)
	if err != nil {
		return nil, nil, err
	}
	var once sync.Once
	owner, err := grpc.NewClient("passthrough:///reference-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		var result net.Conn
		once.Do(func() { result = connection })
		if result == nil {
			return nil, errors.New("reference runtime session was already consumed")
		}
		return result, nil
	}))
	if err != nil {
		return nil, nil, err
	}
	return hostv1.NewAgentRuntimeServiceClient(owner), owner, nil
}

func importReferenceImage(ctx context.Context, client hostv1.AgentRuntimeServiceClient, archive []byte, manifestDigest string) error {
	stream, err := client.ImportImage(ctx)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(archive)
	// A replayed import completes after its header; the Host then ends the
	// stream and Send reports io.EOF. CloseAndRecv returns that result.
	err = stream.Send(&hostv1.ImportImageRequest{Payload: &hostv1.ImportImageRequest_Header{Header: &hostv1.ImportImageHeader{RequestId: "reference-image", ImageReference: referenceImage,
		ArchiveSha256: digest[:], ManifestDigest: manifestDigest, Platform: "linux/arm64", ArchiveSize: uint64(len(archive))}}})
	for err == nil && len(archive) != 0 {
		count := min(len(archive), 32<<10)
		err = stream.Send(&hostv1.ImportImageRequest{Payload: &hostv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: append([]byte(nil), archive[:count]...)}})
		archive = archive[count:]
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func createReferenceContainer(ctx context.Context, client hostv1.AgentRuntimeServiceClient, id, mode string, volumes []*hostv1.VolumeMount) (*hostv1.Container, error) {
	response, err := client.CreateContainer(ctx, &hostv1.CreateContainerRequest{RequestId: "create-" + id, ContainerId: id, Image: referenceImage,
		EntrypointOverride: &hostv1.StringList{Values: []string{"/usr/bin/reference-workload", mode}}, CommandOverride: &hostv1.StringList{}, Volumes: volumes, ReadOnlyRoot: true, LogLimitBytes: 1 << 20})
	if err != nil {
		return nil, err
	}
	return response.GetContainer(), nil
}

func (controller referenceController) routeCapability(exported *servicev1.ServiceExport, allocation *remotev1.Allocation, policyRevision uint64) (*trustv1.RouteCapability, error) {
	state, err := loadReferenceAuthority(filepath.Join(stateDirectory, referenceAuthorityState))
	if err != nil {
		return nil, err
	}
	if exported == nil || allocation == nil || policyRevision == 0 {
		return nil, errors.New("exact reference route identity is required")
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	allocationID := allocation.GetAllocationId()
	capability := &trustv1.RouteCapability{CapabilityId: fmt.Sprintf("reference-route:%d:%s", controller.identity.routeGeneration(), exported.GetExportId()), Authority: proto.Clone(state.anchor.GetAuthority()).(*trustv1.AuthorityDescriptor),
		SourceDeviceId: state.credential.GetSubjectDeviceId(), SourceAllocationId: &allocationID, DestinationDeviceId: state.credential.GetSubjectDeviceId(), DestinationExportId: exported.GetExportId(),
		AllowedProtocol: "tcp", AllowedVerbs: []string{"connect"}, RouteGeneration: controller.identity.routeGeneration(), PolicyRevision: policyRevision,
		Budget: &trustv1.RouteBudget{ConnectionLimit: 1, ConcurrencyLimit: 1, ByteLimit: 1 << 20}, NotBefore: timestamppb.New(now.Add(-time.Second)), ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)),
		Nonce: nonce, Audience: "agent-vmm/service-tunnel/1", IssuerKeyId: state.anchor.GetAuthority().GetKeyId(), RouteClass: "compute", SourceMembershipCredential: proto.Clone(state.credential).(*trustv1.MembershipCredential)}
	capability.Signature, err = signReferenceMessage(state.key, capability)
	return capability, err
}
