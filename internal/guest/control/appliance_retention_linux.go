//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const systemImageRetentionLabel = "agentvmm.system.image-retention"

type systemImageRetention struct {
	Revision uint64   `json:"revision"`
	Digests  []string `json:"digests"`
}

func systemInstallationNamespace(installationID string) string {
	identity := sha256.Sum256([]byte(installationID))
	return "system." + hex.EncodeToString(identity[:16])
}

// The namespace lifecycle lock covers both this projection and image pruning.
func (r *Runtime) projectSystemImageRetention(ctx context.Context, namespace string, projection *vmmdv1.SystemImageRetention) error {
	if projection == nil || projection.GetInstallationRevision() == 0 {
		return errors.New("system image retention requires the installation revision")
	}
	next := systemImageRetention{Revision: projection.GetInstallationRevision(), Digests: slices.Clone(projection.GetManifestDigests())}
	slices.Sort(next.Digests)
	next.Digests = slices.Compact(next.Digests)
	for _, value := range next.Digests {
		if _, err := digest.Parse(value); err != nil {
			return err
		}
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespace)
	if err != nil {
		return err
	}
	var prior systemImageRetention
	if encoded := labels[systemImageRetentionLabel]; encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &prior); err != nil {
			return err
		}
	}
	if next.Revision < prior.Revision {
		return status.Error(codes.Aborted, "system image retention has an older installation revision")
	}
	if next.Revision == prior.Revision && !slices.Equal(next.Digests, prior.Digests) {
		return status.Error(codes.Aborted, "system image retention revision has different facts")
	}
	if next.Revision == prior.Revision {
		return nil
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	return r.client.NamespaceService().SetLabel(ctx, namespace, systemImageRetentionLabel, string(encoded))
}

func (server *Server) ReconcileSystemImages(ctx context.Context, request *vmmdv1.ReconcileSystemImagesRequest) (*vmmdv1.ReconcileSystemImagesResponse, error) {
	value, err := server.mutate(ctx, "reconcile-system-images", request, func() (any, error) {
		if err := validateID("installation_id", request.GetInstallationId()); err != nil {
			return nil, err
		}
		namespace := systemInstallationNamespace(request.GetInstallationId())
		unlock, available := server.runtime.tryLockResource("namespace\x00" + namespace)
		if !available {
			return nil, status.Error(codes.Unavailable, "system image retention deferred while namespace lifecycle is busy")
		}
		defer func() {
			if unlock != nil {
				unlock()
			}
		}()
		if err := server.runtime.namespaceExistsForRole(ctx, namespace, true); err != nil {
			if errdefs.IsNotFound(err) {
				return &vmmdv1.ReconcileSystemImagesResponse{}, nil
			}
			return nil, err
		}
		if err := server.runtime.projectSystemImageRetention(ctx, namespace, request.GetRetention()); err != nil {
			return nil, err
		}
		inventory, err := server.runtime.requireRuntimeInventory()
		if err != nil {
			return nil, err
		}
		inventory.mu.RLock()
		needsPrune := false
		for _, imageDigest := range inventory.images[namespace] {
			if !slices.Contains(request.GetRetention().GetManifestDigests(), imageDigest) {
				needsPrune = true
				break
			}
		}
		inventory.mu.RUnlock()
		if !needsPrune {
			return &vmmdv1.ReconcileSystemImagesResponse{}, nil
		}
		unlock()
		unlock = nil
		available = server.runtime.materializationSlot.TryAcquire(1)
		unlockMaterializer := func() { server.runtime.materializationSlot.Release(1) }
		if !available {
			return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SYSTEM_IMAGE_COLLECTION_DEFERRED, "system image collection deferred while materializer is busy", hostv1.WithResource("system_namespace", namespace), hostv1.WithMetadata(map[string]string{"projection_applied": "true"}))
		}
		defer unlockMaterializer()
		namespaceUnlock, err := server.runtime.lockNamespace(ctx, namespace)
		if err != nil {
			return nil, err
		}
		defer namespaceUnlock()

		if err := server.runtime.pruneSystemImages(ctx, namespace, "", ""); err != nil {
			return nil, hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_SYSTEM_IMAGE_COLLECTION_DEFERRED, "system image projection applied; collection failed: "+err.Error(), hostv1.WithResource("system_namespace", namespace), hostv1.WithMetadata(map[string]string{"projection_applied": "true"}))
		}
		return &vmmdv1.ReconcileSystemImagesResponse{}, nil
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.ReconcileSystemImagesResponse), nil
}
