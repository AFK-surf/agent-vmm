//go:build linux

package control

import (
	"errors"
	"io"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const applianceBuildNamespace = "system.appliance-build"

var applianceBuildLimits = &vmmdv1.NamespaceLimits{CpuMillis: 2000, MemoryBytes: 4 << 30, Pids: 1024, DiskBytes: 64 << 30}

// BuildAppliance is an operator-only build lane. Unlike BuildImage, its OCI
// archive is returned to the host and never becomes an allocation image.
func (s *Server) BuildAppliance(stream grpc.BidiStreamingServer[vmmdv1.BuildApplianceRequest, vmmdv1.BuildApplianceResponse]) error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "vmmd is shutting down")
	}
	return rpcError(s.runtime.BuildAppliance(stream))
}

func (r *Runtime) BuildAppliance(stream grpc.BidiStreamingServer[vmmdv1.BuildApplianceRequest, vmmdv1.BuildApplianceResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || validateRequestID(header.GetRequestId()) != nil {
		return errors.New("first appliance build event must be a valid header")
	}
	header = &vmmdv1.BuildImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: applianceBuildNamespace,
		DockerfilePath: header.GetDockerfilePath(), Image: "agent-vmm.local/appliance-build:" + header.GetRequestId(), Target: header.Target,
		BuildArgs: header.GetBuildArgs(), NetworkMode: header.GetNetworkMode(), Secrets: header.GetSecrets()}
	if _, err := r.ensureNamespace(stream.Context(), &vmmdv1.EnsureNamespaceRequest{RequestId: header.GetRequestId(), NamespaceId: applianceBuildNamespace,
		Limits: applianceBuildLimits, EgressMode: vmmdv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}, true); err != nil {
		return err
	}
	workspace, err := r.builder.Begin(header)
	if err != nil {
		return err
	}
	defer workspace.Close()
	if err := stream.Send(&vmmdv1.BuildApplianceResponse{Event: &vmmdv1.BuildApplianceResponse_Progress{Progress: "receiving context"}}); err != nil {
		return err
	}
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch event := request.GetEvent().(type) {
		case *vmmdv1.BuildApplianceRequest_Context:
			if err := workspace.AppendContext(event.Context.GetData(), event.Context.GetFinal()); err != nil {
				return err
			}
		case *vmmdv1.BuildApplianceRequest_Secret:
			if err := workspace.AppendSecret(event.Secret.GetId(), event.Secret.GetData(), event.Secret.GetFinal()); err != nil {
				return err
			}
		default:
			return errors.New("appliance build event is missing or out of order")
		}
	}
	if err := stream.Send(&vmmdv1.BuildApplianceResponse{Event: &vmmdv1.BuildApplianceResponse_Progress{Progress: "building in isolated sandbox"}}); err != nil {
		return err
	}
	result, err := workspace.RunArchive(stream.Context(), applianceBuildLimits, func(progress string) error {
		return stream.Send(&vmmdv1.BuildApplianceResponse{Event: &vmmdv1.BuildApplianceResponse_Progress{Progress: progress}})
	}, applianceArchiveWriter{stream: stream})
	if err != nil {
		return err
	}
	return stream.Send(&vmmdv1.BuildApplianceResponse{Event: &vmmdv1.BuildApplianceResponse_Result{Result: &vmmdv1.BuildApplianceResult{
		ArchiveSha256: result.ArchiveSHA256, ManifestDigest: result.ManifestDigest, ArchiveSize: result.Size, CacheHit: result.CacheHit}}})
}

type applianceArchiveWriter struct {
	stream grpc.BidiStreamingServer[vmmdv1.BuildApplianceRequest, vmmdv1.BuildApplianceResponse]
}

func (writer applianceArchiveWriter) Write(data []byte) (int, error) {
	written := len(data)
	for len(data) != 0 {
		count := min(len(data), 1<<20)
		if err := writer.stream.Send(&vmmdv1.BuildApplianceResponse{Event: &vmmdv1.BuildApplianceResponse_ArchiveChunk{ArchiveChunk: append([]byte(nil), data[:count]...)}}); err != nil {
			return 0, err
		}
		data = data[count:]
	}
	return written, nil
}
