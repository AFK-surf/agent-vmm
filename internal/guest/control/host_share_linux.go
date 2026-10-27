//go:build linux

package control

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const hostShareRoot = "/run/agent-vmm/hostshares"
const hostShareTag = "agent.hostshares"

func (s *Server) ApplyHostShares(ctx context.Context, request *vmmdv1.ApplyHostSharesRequest) (*vmmdv1.ApplyHostSharesResponse, error) {
	if s.shuttingDown.Load() {
		return nil, status.Error(codes.Unavailable, "Guest is stopping")
	}
	response, err := s.runtime.applyHostShares(ctx, request.GetShares())
	return response, rpcError(err)
}

func (r *Runtime) applyHostShares(ctx context.Context, shares []*vmmdv1.HostShareProjection) (*vmmdv1.ApplyHostSharesResponse, error) {
	r.hostShareMu.Lock()
	defer r.hostShareMu.Unlock()
	// Clear authority before observing a replacement. Failure never preserves a stale grant.
	r.hostShares = nil
	if len(shares) == 0 {
		return &vmmdv1.ApplyHostSharesResponse{}, nil
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(hostShareRoot, &stat); err != nil || stat.Type != unix.FUSE_SUPER_MAGIC {
		if err := os.MkdirAll(hostShareRoot, 0700); err != nil {
			return nil, err
		}
		if output, err := exec.CommandContext(ctx, "/sbin/modprobe", "virtiofs").CombinedOutput(); err != nil {
			return nil, fmt.Errorf("load host share filesystem: %w: %s", err, output)
		}
		if err := unix.Mount(hostShareTag, hostShareRoot, "virtiofs", unix.MS_NOSUID|unix.MS_NODEV, ""); err != nil {
			return nil, fmt.Errorf("mount host shares: %w", err)
		}
	}
	current, err := observeHostShareProjection(hostShareRoot, shares)
	r.hostShares = current
	if err != nil {
		return nil, err
	}
	response := &vmmdv1.ApplyHostSharesResponse{}
	for id := range current {
		response.AppliedShareIds = append(response.AppliedShareIds, id)
	}
	slices.Sort(response.AppliedShareIds)
	return response, nil
}

func observeHostShareProjection(root string, shares []*vmmdv1.HostShareProjection) (map[string]*vmmdv1.HostShareProjection, error) {
	current := make(map[string]*vmmdv1.HostShareProjection, len(shares))
	for _, share := range shares {
		if err := validateID("share_id", share.GetShareId()); err != nil {
			return nil, err
		}
		if err := validateID("actor_namespace_id", share.GetActorNamespaceId()); err != nil {
			return nil, err
		}
		if share.GetRevision() == 0 {
			return nil, fmt.Errorf("host share revision is missing")
		}
		path := filepath.Join(root, share.GetShareId())
		info, err := os.Lstat(path)
		if err == nil && !info.IsDir() {
			err = fmt.Errorf("host share %s is not a directory", share.GetShareId())
		}
		if err == nil {
			var stat unix.Statfs_t
			err = unix.Statfs(path, &stat)
		}
		if err != nil {
			slog.Default().Warn("host share directory is unavailable", "share_id", share.GetShareId(), "path", path, "error", err)
			continue
		}
		current[share.GetShareId()] = proto.Clone(share).(*vmmdv1.HostShareProjection)
	}
	return current, nil
}

func (r *Runtime) mountSource(namespace string, mount *vmmdv1.VolumeMount) (string, error) {
	switch source := mount.GetSource().(type) {
	case *vmmdv1.VolumeMount_OwnedVolume:
		if err := validateID("owned_volume", source.OwnedVolume); err != nil {
			return "", err
		}
		path := volumePath(namespace, source.OwnedVolume)

		return path, nil
	case *vmmdv1.VolumeMount_HostShare:
		r.hostShareMu.RLock()
		defer r.hostShareMu.RUnlock()
		share := r.hostShares[source.HostShare]
		if share == nil || share.GetActorNamespaceId() != namespace || (share.GetReadOnly() && !mount.GetReadOnly()) {
			return "", status.Error(codes.PermissionDenied, "host share is unavailable or not authorized for this environment and access mode")
		}
		path := filepath.Join(hostShareRoot, source.HostShare)
		var stat unix.Statfs_t
		if err := unix.Statfs(path, &stat); err != nil {
			return "", status.Error(codes.FailedPrecondition, err.Error())
		}
		return path, nil
	default:
		return "", status.Error(codes.InvalidArgument, "owned volume or host share is required")
	}
}

func encodeContainerMounts(mounts []*vmmdv1.VolumeMount) (string, error) {
	body, err := protojson.Marshal(&vmmdv1.Container{Volumes: mounts})
	return string(body), err
}

func decodeContainerMounts(label string) ([]*vmmdv1.VolumeMount, error) {
	value := new(vmmdv1.Container)
	err := protojson.Unmarshal([]byte(label), value)
	return value.GetVolumes(), err
}
