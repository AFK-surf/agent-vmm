//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const disposalBatchSize = 32

// Disposal consumes a bounded slice of an isolated namespace. Host owns the durable authorization.
func (s *Server) DisposeNamespaceBatch(ctx context.Context, request *vmmdv1.DisposeNamespaceBatchRequest) (*vmmdv1.DisposeNamespaceBatchResponse, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, rpcError(err)
	}
	if err := validateID("namespace_id", request.GetNamespaceId()); err != nil {
		return nil, rpcError(err)
	}
	complete, stage, err := s.runtime.disposeNamespaceBatch(ctx, request.GetNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.DisposeNamespaceBatchResponse{Complete: complete, Stage: stage}, nil
}

func (r *Runtime) disposeNamespaceBatch(ctx context.Context, namespaceID string) (bool, string, error) {
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		if status.Code(err) == codes.NotFound || errdefs.IsNotFound(err) {
			return true, "complete", nil
		}
		return false, "namespace", err
	}
	// Session cancellation closes accepted builds. Do not remove a namespace while its builder still writes.
	if err := r.builder.CanDeleteNamespace(namespaceID); err != nil {
		return false, "builds", err
	}
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return false, "inventory", err
	}
	ids, err := inventory.disposalBatch(namespaceID, false)
	if err != nil {
		return false, "containers", err
	}
	if len(ids) > 0 {
		for _, id := range ids {
			stopped, err := r.StopContainer(ctx, namespaceID, id, 5*time.Second)
			if err != nil {
				return false, "stopping", err
			}
			if stopped.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_STOPPED && stopped.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_CREATED {
				return false, "stopping", errors.New("container stop is not confirmed")
			}
			if err := r.DeleteContainer(ctx, namespaceID, id, stopped.GetGenerationId()); err != nil {
				return false, "containers", err
			}
		}
		return false, "containers", nil
	}

	directory, err := os.Open(volumePath(namespaceID, ""))
	var entries []os.DirEntry
	if err == nil {
		entries, err = directory.ReadDir(disposalBatchSize)
		closeErr := directory.Close()
		if err != nil && !errors.Is(err, io.EOF) {
			return false, "volumes", err
		}
		if closeErr != nil {
			return false, "volumes", closeErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, "volumes", err
	}
	if len(entries) > 0 {
		for _, entry := range entries {
			if !entry.IsDir() {
				return false, "volumes", errors.New("volume inventory contains a non-directory entry")
			}
			generation, err := volumeGenerationID(volumePath(namespaceID, entry.Name()))
			if err != nil {
				return false, "volumes", err
			}
			if err := r.DeleteVolume(ctx, namespaceID, entry.Name(), generation); err != nil {
				return false, "volumes", err
			}
		}
		return false, "volumes", nil
	}
	references, err := inventory.disposalBatch(namespaceID, true)
	if err != nil {
		return false, "images", err
	}
	if len(references) > 0 {
		for _, reference := range references {
			image, err := r.GetImage(ctx, namespaceID, reference)
			if err != nil {
				return false, "images", err
			}
			if err := r.DeleteImage(ctx, namespaceID, reference, image.GetGenerationId(), image.GetDigest()); err != nil {
				return false, "images", err
			}
		}
		return false, "images", nil
	}
	if err := r.DeleteNamespace(ctx, namespaceID); err != nil {
		return false, "namespace", err
	}
	return true, "complete", nil
}

func (index *runtimeInventoryIndex) disposalBatch(namespaceID string, images bool) ([]string, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	result := make([]string, 0, disposalBatchSize)
	if images {
		values, ok := index.images[namespaceID]
		if !ok {
			return nil, errors.New("runtime namespace is not indexed")
		}
		for id := range values {
			result = append(result, id)
			if len(result) == disposalBatchSize {
				break
			}
		}
	} else {
		values, ok := index.containers[namespaceID]
		if !ok {
			return nil, errors.New("runtime namespace is not indexed")
		}
		values.Ascend(func(id string) bool { result = append(result, id); return len(result) < disposalBatchSize })
	}
	return result, nil
}
