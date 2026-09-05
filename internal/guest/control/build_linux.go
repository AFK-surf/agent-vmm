//go:build linux

package control

import (
	"errors"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"io"
	"os"

	"github.com/opencontainers/go-digest"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) BuildImage(stream grpc.BidiStreamingServer[vmmdv1.BuildImageRequest, vmmdv1.BuildImageResponse]) error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "vmmd is shutting down")
	}
	return rpcError(s.runtime.BuildImage(stream))
}

func (r *Runtime) BuildImage(stream grpc.BidiStreamingServer[vmmdv1.BuildImageRequest, vmmdv1.BuildImageResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	header := request.GetHeader()
	if header == nil {
		return errors.New("first build event must be a header")
	}
	if err := validateRequestID(header.GetRequestId()); err != nil {
		return err
	}
	if err := validateID("actor_namespace_id", header.GetActorNamespaceId()); err != nil {
		return err
	}
	unlock := r.lockNamespace(header.GetActorNamespaceId())
	labels, err := r.client.NamespaceService().Labels(stream.Context(), header.GetActorNamespaceId())
	if err != nil {
		unlock()
		return err
	}
	_, namespace, err := namespaceFromLabels(header.GetActorNamespaceId(), labels)
	if err != nil {
		unlock()
		return err
	}
	workspace, err := r.builder.Begin(header)
	unlock()
	if err != nil {
		return err
	}
	defer workspace.Close()
	if err := stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: "receiving context"}}); err != nil {
		return err
	}
	for {
		request, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch event := request.GetEvent().(type) {
		case *vmmdv1.BuildImageRequest_Context:
			if err := workspace.AppendContext(event.Context.GetData(), event.Context.GetFinal()); err != nil {
				return err
			}
		case *vmmdv1.BuildImageRequest_Secret:
			if err := workspace.AppendSecret(event.Secret.GetId(), event.Secret.GetData(), event.Secret.GetFinal()); err != nil {
				return err
			}
		default:
			return errors.New("build event is missing or out of order")
		}
	}
	if err := stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: "building in isolated sandbox"}}); err != nil {
		return err
	}
	return workspace.RunArchive(stream.Context(), namespace.GetLimits(), func(progress string) error {
		return stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: progress}})
	}, func(archive *os.File, result guestbuild.ArchiveResult) error {
		reference, err := normalizeImageReference(header.GetImage())
		if err != nil {
			return err
		}
		manifest, err := validateOCIArchive(archive, digest.Digest(result.ManifestDigest))
		if err != nil {
			return err
		}
		importHeader := &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: header.GetActorNamespaceId(),
			ImageReference: reference, ManifestDigest: result.ManifestDigest, Platform: "linux/arm64",
			ArchiveSize: result.Size, ArchiveSha256: result.ArchiveSHA256}
		// OCIImageMaterialization.tla: runtime import precedes unpack and tag publication.
		err = r.guardOCIPhysicalGrowth(stream.Context(), "build-output\x00"+header.GetActorNamespaceId()+"\x00"+header.GetRequestId(), true, func() error {
			_, importErr := r.importImage(stream.Context(), archive, importHeader, reference, *manifest, false, true)
			return importErr
		})
		if err != nil {
			return err
		}
		if result.CacheHit {
			if err := stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: "namespace cache hit"}}); err != nil {
				return err
			}
		}
		return stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Result{Result: &vmmdv1.BuildImageResult{Image: header.GetImage(), Digest: result.ManifestDigest}}})
	})
}
