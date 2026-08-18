//go:build darwin

package host

import (
	"errors"
	"io"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

// BuildArtifact relays a Dockerfile context to the guest's build-only system
// namespace. The archive is returned directly to the local operator; it is
// not imported into a workload namespace or retained by the build service.
func (server *applianceAdminServer) BuildArtifact(stream appliancev1.ApplianceAdminService_BuildArtifactServer) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.BuildAppliance(stream.Context())
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader() == nil {
		return invalidField("header", "first appliance build event must be a header")
	}
	if err := guest.Send(&vmmdv1.BuildApplianceRequest{Event: &vmmdv1.BuildApplianceRequest_Header{Header: applianceBuildHeaderToGuest(first.GetHeader())}}); err != nil {
		return err
	}
	receiveError := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				receiveError <- guest.CloseSend()
				return
			}
			if err != nil {
				receiveError <- err
				return
			}
			converted, err := applianceBuildRequestToGuest(request)
			if err == nil {
				err = guest.Send(converted)
			}
			if err != nil {
				receiveError <- err
				return
			}
		}
	}()
	for {
		response, err := guest.Recv()
		if errors.Is(err, io.EOF) {
			return streamReceiveResult(receiveError)
		}
		if err != nil {
			return err
		}
		if err := stream.Send(applianceBuildResponseFromGuest(response)); err != nil {
			return err
		}
	}
}

func applianceBuildHeaderToGuest(value *appliancev1.BuildArtifactHeader) *vmmdv1.BuildImageHeader {
	result := &vmmdv1.BuildImageHeader{RequestId: value.GetRequestId(), DockerfilePath: value.GetDockerfilePath(), Target: value.Target,
		NetworkMode: vmmdv1.BuildNetworkMode(value.GetNetworkMode())}
	for _, argument := range value.GetBuildArgs() {
		result.BuildArgs = append(result.BuildArgs, &vmmdv1.BuildArgument{Name: argument.GetName(), Value: argument.GetValue()})
	}
	for _, secret := range value.GetSecrets() {
		result.Secrets = append(result.Secrets, &vmmdv1.BuildSecret{Id: secret.GetId()})
	}
	return result
}

func applianceBuildRequestToGuest(value *appliancev1.BuildArtifactRequest) (*vmmdv1.BuildApplianceRequest, error) {
	switch event := value.GetEvent().(type) {
	case *appliancev1.BuildArtifactRequest_Context:
		return &vmmdv1.BuildApplianceRequest{Event: &vmmdv1.BuildApplianceRequest_Context{Context: &vmmdv1.BuildContextChunk{Data: event.Context.GetData(), Final: event.Context.GetFinal()}}}, nil
	case *appliancev1.BuildArtifactRequest_Secret:
		return &vmmdv1.BuildApplianceRequest{Event: &vmmdv1.BuildApplianceRequest_Secret{Secret: &vmmdv1.BuildSecretChunk{Id: event.Secret.GetId(), Data: event.Secret.GetData(), Final: event.Secret.GetFinal()}}}, nil
	default:
		return nil, invalidField("event", "appliance build event is missing or out of order")
	}
}

func applianceBuildResponseFromGuest(value *vmmdv1.BuildApplianceResponse) *appliancev1.BuildArtifactResponse {
	result := &appliancev1.BuildArtifactResponse{}
	switch event := value.GetEvent().(type) {
	case *vmmdv1.BuildApplianceResponse_Progress:
		result.Event = &appliancev1.BuildArtifactResponse_Progress{Progress: event.Progress}
	case *vmmdv1.BuildApplianceResponse_ArchiveChunk:
		result.Event = &appliancev1.BuildArtifactResponse_ArchiveChunk{ArchiveChunk: event.ArchiveChunk}
	case *vmmdv1.BuildApplianceResponse_Result:
		result.Event = &appliancev1.BuildArtifactResponse_Result{Result: &appliancev1.BuildArtifactResult{ArchiveSha256: event.Result.GetArchiveSha256(), ManifestDigest: event.Result.GetManifestDigest(), ArchiveSize: event.Result.GetArchiveSize(), CacheHit: event.Result.GetCacheHit()}}
	}
	return result
}
