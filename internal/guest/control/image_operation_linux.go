//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type imageOperationRun struct {
	request *vmmdv1.ImageOperationRequest // Nil for archive-only appliance builds, which have no image receipt.
	cancel  context.CancelFunc
	done    chan struct{}
	cleanup func(context.Context) error
}

func imageOperationKey(request string, kind vmmdv1.ImageOperationKind) string {
	return fmt.Sprintf("%simage-operation.%d.%x", labelPrefix, kind, sha256.Sum256([]byte(request)))
}

func imageOperationRecordKey(namespace, request string, kind vmmdv1.ImageOperationKind) string {
	return fmt.Sprintf("%simage-operation.%x.%d.%x", labelPrefix, sha256.Sum256([]byte(namespace)), kind, sha256.Sum256([]byte(request)))
}

func (r *Runtime) readImageOperation(ctx context.Context, namespace, request string, kind vmmdv1.ImageOperationKind) (*vmmdv1.ImageOperationReceipt, string, error) {
	labels, err := r.client.NamespaceService().Labels(ctx, runtimeContainerdNamespace)
	if err != nil {
		return nil, "", err
	}
	text := labels[imageOperationRecordKey(namespace, request, kind)]
	if text == "" {
		return nil, "", status.Error(codes.NotFound, "image operation has not been accepted")
	}
	var record imageReceiptLabel
	if err := json.Unmarshal([]byte(text), &record); err != nil {
		return nil, "", err
	}
	bytes, err := base64.StdEncoding.DecodeString(record.Receipt)
	if err != nil {
		return nil, "", err
	}
	receipt := new(vmmdv1.ImageOperationReceipt)
	if err := proto.Unmarshal(bytes, receipt); err != nil {
		return nil, "", err
	}
	return receipt, record.WriterID, nil
}
func (r *Runtime) imageOperation(ctx context.Context, namespace, request string, kind vmmdv1.ImageOperationKind) (*vmmdv1.ImageOperationReceipt, error) {
	receipt, _, err := r.readImageOperation(ctx, namespace, request, kind)
	return receipt, err
}
func (r *Runtime) saveImageOperation(ctx context.Context, namespace string, receipt *vmmdv1.ImageOperationReceipt, sourceImage string) error {
	writerID := r.imageOperationWriterID
	if _, oldWriter, err := r.readImageOperation(ctx, namespace, receipt.RequestId, receipt.Kind); err == nil {
		writerID = oldWriter
		labels, err := r.client.NamespaceService().Labels(ctx, runtimeContainerdNamespace)
		if err != nil {
			return err
		}
		var prior imageReceiptLabel
		if err := json.Unmarshal([]byte(labels[imageOperationRecordKey(namespace, receipt.RequestId, receipt.Kind)]), &prior); err != nil {
			return err
		}
		sourceImage = prior.SourceImage
	} else if status.Code(err) != codes.NotFound {
		return err
	}
	key := imageOperationRecordKey(namespace, receipt.RequestId, receipt.Kind)
	record, err := encodeImageReceiptLabel(key, receipt, imageReceiptLabel{WriterID: writerID, SourceImage: sourceImage})
	if err != nil {
		return err
	}
	return r.client.NamespaceService().SetLabel(ctx, runtimeContainerdNamespace, key, record)
}

func operationIdentityMatches(receipt *vmmdv1.ImageOperationReceipt, request *vmmdv1.ImageOperationRequest) bool {
	return receipt.GetRequestId() == request.GetRequestId() && receipt.GetKind() == request.GetKind() && receipt.GetInputSha256() == request.GetInputSha256() && receipt.GetExecutionId() == request.GetExecutionId() && receipt.GetContainerId() == request.GetContainerId() && receipt.GetExpectedInstanceId() == request.GetExpectedInstanceId()
}

func (s *Server) ObserveImageOperation(ctx context.Context, request *vmmdv1.ImageOperationRequest) (*vmmdv1.ImageOperationReceipt, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, rpcError(err)
	}
	if err := validateID("namespace_id", request.GetActorNamespaceId()); err != nil {
		return nil, rpcError(err)
	}
	key := request.ActorNamespaceId + "\x00" + imageOperationKey(request.RequestId, request.Kind)
	value, _ := s.runtime.resourceLocks.LoadOrStore(key, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	receipt, writerID, err := s.runtime.readImageOperation(ctx, request.ActorNamespaceId, request.RequestId, request.Kind)
	if err != nil {
		return nil, rpcError(err)
	}
	if !operationIdentityMatches(receipt, request) {
		return nil, status.Error(codes.Aborted, "image operation identity differs from accepted input")
	}
	if receipt.WriterSettled {
		return receipt, nil
	}
	var run *imageOperationRun
	if value, found := s.runtime.imageOperations.Load(key); found {
		run = value.(*imageOperationRun)
		select {
		case <-run.done:
		default:
			return receipt, nil
		}
	}
	published := false
	if reference, normalizeErr := normalizeImageReference(receipt.ImageReference); normalizeErr == nil {
		if image, lookupErr := s.runtime.client.ImageService().Get(namespaceContext(ctx, request.ActorNamespaceId), reference); lookupErr == nil {
			kind := "copy"
			if request.Kind == vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD {
				kind = "build"
			}
			provenance := imageOperationReceipt(image, kind)
			if provenance != nil && provenance.RequestId == receipt.RequestId && provenance.InputSha256 == receipt.InputSha256 {
				published = true
				receipt.ManifestDigest = image.Target.Digest.String()
			}
		} else if !errdefs.IsNotFound(lookupErr) {
			return nil, rpcError(lookupErr)
		}
	}
	if run == nil && !published && (!s.runtime.imageOperationsRecovered || writerID == "" || writerID == s.runtime.imageOperationWriterID) {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
		receipt.Status = status.New(codes.Unknown, "image execution handler was lost while its containerd writer still exists; writer settlement awaits daemon recovery").Proto()
		return receipt, nil
	}
	if request.Kind == vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD {
		if run != nil && run.cleanup != nil {
			err = run.cleanup(ctx)
		} else {
			err = s.runtime.builder.CleanupExecution(ctx, request.ActorNamespaceId, receipt.RuntimeInstanceId)
		}
	}
	if err == nil {
		err = s.runtime.cleanupImageOperationLease(ctx, request.ActorNamespaceId, request.RequestId, request.Kind)
	}
	if err != nil {
		return receipt, rpcError(err)
	}
	if receipt.State == vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_RUNNING {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
	}
	if published {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED
		receipt.Status = nil
	}
	receipt.WriterSettled = true
	if receipt.Status == nil && !published {
		receipt.Status = status.New(codes.Unknown, "guest execution result was lost; exact writer and temporary resources have been settled").Proto()
	}
	if err := s.runtime.retainSuccessfulBuild(ctx, request.ActorNamespaceId, receipt); err != nil {
		return nil, rpcError(err)
	}
	if err := s.runtime.saveImageOperation(ctx, request.ActorNamespaceId, receipt, ""); err != nil {
		return nil, rpcError(err)
	}
	s.runtime.imageOperations.Delete(key)
	return receipt, nil
}

func (s *Server) CancelImageOperation(ctx context.Context, request *vmmdv1.ImageOperationRequest) (*vmmdv1.ImageOperationReceipt, error) {
	receipt, err := s.ObserveImageOperation(ctx, request)
	if err != nil || receipt.WriterSettled {
		return receipt, err
	}
	key := request.ActorNamespaceId + "\x00" + imageOperationKey(request.RequestId, request.Kind)
	if value, found := s.runtime.imageOperations.Load(key); found {
		run := value.(*imageOperationRun)
		run.cancel()
		select {
		case <-run.done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return s.ObserveImageOperation(ctx, request)
}

func (r *Runtime) acceptImageOperation(ctx context.Context, namespace string, receipt *vmmdv1.ImageOperationReceipt, sourceImage string, cancel context.CancelFunc) (*imageOperationRun, *vmmdv1.ImageOperationReceipt, error) {
	key := namespace + "\x00" + imageOperationKey(receipt.RequestId, receipt.Kind)
	value, _ := r.resourceLocks.LoadOrStore(key, new(sync.Mutex))
	lock := value.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.imageOperationsClosing {
		return nil, nil, status.Error(codes.Unavailable, "vmmd is shutting down")
	}
	request := &vmmdv1.ImageOperationRequest{ActorNamespaceId: namespace, RequestId: receipt.RequestId, Kind: receipt.Kind, InputSha256: receipt.InputSha256, ExecutionId: receipt.ExecutionId, ContainerId: receipt.ContainerId, ExpectedInstanceId: receipt.ExpectedInstanceId}
	prior, err := r.imageOperation(ctx, namespace, receipt.RequestId, receipt.Kind)
	if err == nil {
		if !operationIdentityMatches(prior, request) {
			return nil, nil, status.Error(codes.Aborted, "image operation request was reused with different input")
		}
		return nil, prior, nil
	}
	if status.Code(err) != codes.NotFound {
		return nil, nil, err
	}
	receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_RUNNING
	if err := r.saveImageOperation(ctx, namespace, receipt, sourceImage); err != nil {
		return nil, nil, err
	}
	run := &imageOperationRun{request: request, cancel: cancel, done: make(chan struct{})}
	r.imageOperations.Store(key, run)
	return run, nil, nil
}

func (r *Runtime) finishImageOperation(namespace string, receipt *vmmdv1.ImageOperationReceipt, run *imageOperationRun, operationErr, cleanupErr error, canceled bool) error {
	var failure *imageCleanupFailure
	if errors.As(operationErr, &failure) {
		cleanupErr = errors.Join(cleanupErr, failure)
	}
	receipt.WriterSettled = cleanupErr == nil
	if operationErr == nil {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED
	} else if canceled && cleanupErr != nil {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
	} else if canceled {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED
	} else {
		receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED
	}
	if operationErr != nil {
		result := status.Convert(operationErr)
		if errors.Is(operationErr, context.Canceled) || errors.Is(operationErr, context.DeadlineExceeded) {
			result = status.FromContextError(operationErr)
		}
		receipt.Status = result.Proto()
	}
	if cleanupErr != nil {
		if receipt.Status == nil {
			receipt.Status = status.New(codes.Unknown, "image writer cleanup failed").Proto()
		}
		receipt.Status.Message += "\ncleanup: " + cleanupErr.Error()
	}

	if err := r.retainSuccessfulBuild(context.Background(), namespace, receipt); err != nil {
		close(run.done)
		return err
	}
	err := r.saveImageOperation(context.Background(), namespace, receipt, "")
	if receipt.WriterSettled && err == nil {
		r.imageOperations.Delete(namespace + "\x00" + imageOperationKey(receipt.RequestId, receipt.Kind))
	}
	close(run.done)
	return err
}

type imageOperationContextKey struct{}
type imageCleanupFailure struct{ error }

func imageOperationLeaseID(namespace, request string, kind vmmdv1.ImageOperationKind) string {
	return fmt.Sprintf("image-operation-%d-%x", kind, sha256.Sum256([]byte(namespace+"\x00"+request)))
}
func (r *Runtime) cleanupImageOperationLease(ctx context.Context, namespace, request string, kind vmmdv1.ImageOperationKind) error {
	err := r.client.LeasesService().Delete(runtimeContext(ctx), leases.Lease{ID: imageOperationLeaseID(namespace, request, kind)}, leases.SynchronousDelete)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *Runtime) unsettledImageReferences(ctx context.Context) ([]imageReceiptLabel, error) {
	labels, err := r.client.NamespaceService().Labels(ctx, runtimeContainerdNamespace)
	if err != nil {
		return nil, err
	}
	var references []imageReceiptLabel
	for key, value := range labels {
		if !strings.HasPrefix(key, labelPrefix+"image-operation.") {
			continue
		}
		var record imageReceiptLabel
		if err := json.Unmarshal([]byte(value), &record); err != nil {
			return nil, err
		}
		if record.SourceImage == "" {
			continue
		}
		bytes, err := base64.StdEncoding.DecodeString(record.Receipt)
		if err != nil {
			return nil, err
		}
		receipt := new(vmmdv1.ImageOperationReceipt)
		if err := proto.Unmarshal(bytes, receipt); err != nil {
			return nil, err
		}
		if !receipt.WriterSettled {
			record.Digest = receipt.ManifestDigest
			references = append(references, record)
		}
	}
	return references, nil
}
