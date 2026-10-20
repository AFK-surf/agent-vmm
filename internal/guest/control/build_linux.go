//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"time"

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

func (r *Runtime) BuildImage(stream grpc.BidiStreamingServer[vmmdv1.BuildImageRequest, vmmdv1.BuildImageResponse]) (operationErr error) {
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
	input := proto.Clone(header).(*vmmdv1.BuildImageHeader)
	input.RequestId, input.ActorNamespaceId, input.ExecutionId, input.ContainerId, input.ExpectedInstanceId = "", "", "", "", ""
	input.ExecutionDeadlineUnixNano = 0
	input.ControllerCacheScope = ""
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(input)
	if err != nil {
		_ = workspace.Close()
		return err
	}
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: header.RequestId, InputSha256: fmt.Sprintf("%x", sha256.Sum256(bytes)), Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, ExecutionId: header.ExecutionId, ContainerId: header.ContainerId, ExpectedInstanceId: header.ExpectedInstanceId, ImageReference: header.Image, RuntimeInstanceId: workspace.ID()}
	operationContext, cancel := context.WithCancel(context.WithoutCancel(stream.Context()))
	if header.ExecutionDeadlineUnixNano != 0 {
		cancel()
		operationContext, cancel = context.WithDeadline(context.WithoutCancel(stream.Context()), time.Unix(0, header.ExecutionDeadlineUnixNano))
	}
	defer cancel()
	run, prior, err := r.acceptImageOperation(context.WithoutCancel(stream.Context()), header.ActorNamespaceId, receipt, "", cancel)
	if err != nil {
		_ = workspace.Close()
		return err
	}
	if prior != nil {
		_ = workspace.Close()
		if prior.State == vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED && prior.WriterSettled {
			return stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Result{Result: &vmmdv1.BuildImageResult{Image: prior.ImageReference, Digest: prior.ManifestDigest}}})
		}
		return status.Error(codes.Aborted, "build was already accepted; observe the original operation")
	}
	operationContext = context.WithValue(operationContext, imageOperationContextKey{}, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD)
	run.cleanup = func(ctx context.Context) error {
		if err := workspace.RecoverClose(ctx); err != nil {
			return err
		}
		return r.settleBuilderCacheReservation(ctx, "cache-write\x00"+workspace.ID())
	}
	defer func() {
		cleanupErr := workspace.Close()
		if cleanupErr == nil {
			cleanupErr = r.settleBuilderCacheReservation(context.Background(), "cache-write\x00"+workspace.ID())
		}
		if err := r.finishImageOperation(header.ActorNamespaceId, receipt, run, operationErr, cleanupErr, operationContext.Err() != nil); err != nil {
			operationErr = errors.Join(operationErr, err)
			return
		}
		if operationErr == nil && cleanupErr == nil {
			operationErr = stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Result{Result: &vmmdv1.BuildImageResult{Image: receipt.ImageReference, Digest: receipt.ManifestDigest}}})
		}
		operationErr = errors.Join(operationErr, cleanupErr)
	}()
	if err := stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: "receiving context"}}); err != nil {
		return err
	}
	for {
		received := make(chan struct {
			request *vmmdv1.BuildImageRequest
			err     error
		}, 1)
		go func() {
			value, err := stream.Recv()
			received <- struct {
				request *vmmdv1.BuildImageRequest
				err     error
			}{value, err}
		}()
		select {
		case value := <-received:
			request, err = value.request, value.err
		case <-operationContext.Done():
			return operationContext.Err()
		}
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
	_ = stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: "building in isolated sandbox"}})
	return workspace.RunArchive(operationContext, namespace.GetLimits(), func(progress string) error {
		_ = stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: progress}})
		return nil
	}, func(archive *os.File, result guestbuild.ArchiveResult) error {
		reference, err := normalizeImageReference(header.GetImage())
		if err != nil {
			return err
		}
		manifest, err := ociarchive.Validate(archive, digest.Digest(result.ManifestDigest))
		if err != nil {
			return err
		}
		importHeader := &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: header.GetActorNamespaceId(),
			ImageReference: reference, ManifestDigest: result.ManifestDigest, Platform: "linux/arm64",
			ArchiveSize: result.Size, ArchiveSha256: result.ArchiveSHA256}
		// OCIImageMaterialization.tla: runtime import precedes unpack and tag publication.
		err = r.guardOCIPhysicalGrowth(operationContext, "build-output\x00"+header.GetActorNamespaceId()+"\x00"+header.GetRequestId(), true, func() error {
			_, importErr := r.importImage(operationContext, archive, importHeader, reference, *manifest, false, true)
			return importErr
		})
		if err != nil {
			return err
		}
		if err := r.recordImageBuildReceipt(operationContext, header.GetActorNamespaceId(), reference, result.ManifestDigest, header.ControllerCacheScope, receipt); err != nil {
			return err
		}
		receipt.ManifestDigest = result.ManifestDigest
		return nil
	})
}
