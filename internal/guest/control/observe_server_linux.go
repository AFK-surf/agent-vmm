//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
)

func (s *Server) GetNamespace(ctx context.Context, request *vmmdv1.GetNamespaceRequest) (*vmmdv1.GetNamespaceResponse, error) {
	namespace, err := s.runtime.GetNamespace(ctx, request.GetNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetNamespaceResponse{Namespace: namespace}, nil
}

func (s *Server) ListNamespaces(ctx context.Context, _ *vmmdv1.ListNamespacesRequest) (*vmmdv1.ListNamespacesResponse, error) {
	namespaces, err := s.runtime.ListNamespaces(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.ListNamespacesResponse{Namespaces: namespaces}, nil
}

func (s *Server) GetNamespaceUsage(ctx context.Context, request *vmmdv1.GetNamespaceUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	usage, err := s.runtime.NamespaceUsage(ctx, request.GetNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetUsageResponse{Usage: usage}, nil
}

func (s *Server) GetSystemUsage(context.Context, *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	usage, err := s.runtime.SystemUsage()
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetUsageResponse{Usage: usage}, nil
}

func (s *Server) GetVolume(ctx context.Context, request *vmmdv1.GetVolumeRequest) (*vmmdv1.GetVolumeResponse, error) {
	volume, err := s.runtime.GetVolume(ctx, request.GetActorNamespaceId(), request.GetVolumeId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetVolumeResponse{Volume: volume}, nil
}

func (s *Server) ListVolumes(ctx context.Context, request *vmmdv1.ListVolumesRequest) (*vmmdv1.ListVolumesResponse, error) {
	volumes, err := s.runtime.ListVolumes(ctx, request.GetActorNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.ListVolumesResponse{Volumes: volumes}, nil
}

func (s *Server) GetImage(ctx context.Context, request *vmmdv1.GetImageRequest) (*vmmdv1.GetImageResponse, error) {
	image, err := s.runtime.GetImage(ctx, request.GetActorNamespaceId(), request.GetReference())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetImageResponse{Image: image}, nil
}

func (s *Server) ListImages(ctx context.Context, request *vmmdv1.ListImagesRequest) (*vmmdv1.ListImagesResponse, error) {
	images, err := s.runtime.ListImages(ctx, request.GetActorNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.ListImagesResponse{Images: images}, nil
}

func (s *Server) GetContainerUsage(ctx context.Context, request *vmmdv1.GetContainerUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	usage, err := s.runtime.ContainerUsage(ctx, request.GetActorNamespaceId(), request.GetContainerId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetUsageResponse{Usage: usage}, nil
}

func (s *Server) ComponentHealth(ctx context.Context, _ *vmmdv1.ComponentHealthRequest) (*vmmdv1.ComponentHealthResponse, error) {
	now := time.Now().UnixNano()
	probes := s.healthProbe(ctx)
	egressStatus := componentStatus("egress", probes.egress, now)
	if probes.egress == nil && s.health.EgressStatus != nil {
		egressStatus.Reason = s.health.EgressStatus()
	}
	return &vmmdv1.ComponentHealthResponse{Components: []*vmmdv1.ComponentStatus{
		componentStatus("vmmd", nil, now),
		componentStatus("state-disk", probes.disk, now),
		componentStatus("network", probes.network, now),
		egressStatus,
		componentStatus("quota", probes.quota, now),
		componentStatus("containerd/runsc", probes.containerd, now),
	}}, nil
}

func (s *Server) ProbeEgress(ctx context.Context, request *vmmdv1.ProbeEgressRequest) (*vmmdv1.ProbeEgressResponse, error) {
	if request.GetNamespaceId() == "" {
		return nil, invalid(errors.New("namespace_id is required"))
	}
	result := s.runtime.network.ProbeEgress(ctx, request.GetNamespaceId())
	return &vmmdv1.ProbeEgressResponse{
		PolicyEnforced: result.PolicyEnforced,
		DnsOk:          result.DNSOK,
		TcpOk:          result.TCPOK,
		PrivateDenied:  result.PrivateDenied,
		FailureLayer:   result.FailureLayer,
		Reason:         result.Reason,
	}, nil
}

type healthProbeResult struct {
	disk       error
	network    error
	egress     error
	quota      error
	containerd error
}

func (s *Server) probeComponents(ctx context.Context) healthProbeResult {
	var disk syscall.Statfs_t
	diskErr := syscall.Statfs(stateRoot, &disk)
	if diskErr == nil && disk.Blocks == 0 {
		diskErr = fmt.Errorf("state filesystem reports zero blocks")
	}
	_, quotaErr := projectUsage(s.runtime.stateDevice, 0)
	_, containerdErr := s.runtime.client.Version(ctx)
	var egressErr error
	if s.health.EgressHealth != nil {
		egressErr = s.health.EgressHealth()
	}
	return healthProbeResult{
		disk:       errors.Join(diskErr, pathWritable(stateRoot)),
		network:    s.runtime.network.Health(),
		egress:     egressErr,
		quota:      quotaErr,
		containerd: containerdErr,
	}
}

func pathWritable(path string) error {
	file, err := os.CreateTemp(path, ".health-*")
	if err != nil {
		return err
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		return err
	}
	return os.Remove(name)
}

func (s *Server) WatchEvents(request *vmmdv1.WatchEventsRequest, stream grpc.ServerStreamingServer[vmmdv1.RuntimeEvent]) error {
	return s.events.watch(stream.Context(), request.GetAfterSequence(), stream.Send)
}
