//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func (s *Server) GetSystemUsage(ctx context.Context, _ *vmmdv1.GetSystemUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	usage, err := s.runtime.SystemUsage()
	if err != nil {
		usage = unavailableUsage(err)
	}
	response := &vmmdv1.GetUsageResponse{Usage: usage}
	if openFDs, openFDErr := guestOpenFDs("/proc/sys/fs/file-nr"); openFDErr == nil {
		response.OpenFds = &openFDs
	} else {
		response.OpenFdsUnavailableReason = boundedObservationFailure("Guest file descriptors", openFDErr)
	}
	if disk, diskErr := s.runtime.guestDiskSnapshot(ctx); diskErr == nil {
		response.Disk = &vmmdv1.DiskAccounting{
			UsedBytes:              disk.UsedBytes,
			CapacityBytes:          disk.CapacityBytes,
			ImportReservationBytes: disk.ImportReservationBytes,
			SystemReserveBytes:     disk.EmergencyBytes,
			MetadataReserveBytes:   disk.MetadataGrowthBytes,
		}
	} else {
		response.DiskUnavailableReason = boundedObservationFailure("Guest disk", diskErr)
	}
	pool, limit, pressure, kills, oom, err := workloadPoolUsage("/sys/fs/cgroup/agent-vmm/workload")
	if err != nil {
		response.WorkloadPool = unavailableUsage(err)
	} else {
		response.WorkloadPool = pool
		response.WorkloadMemoryLimitBytes = limit
		response.WorkloadMemoryFullAvg10 = pressure
		response.WorkloadOomKills = kills
		response.WorkloadOomEvents = oom
	}
	return response, nil
}

func boundedObservationFailure(layer string, failure error) string {
	message := layer + ": " + failure.Error()
	if len(message) > 1024 {
		return message[:1024]
	}
	return message
}

func guestOpenFDs(path string) (uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0, errors.New("Guest file descriptor usage is unavailable")
	}
	value, err := strconv.ParseUint(fields[0], 10, 32)
	return uint32(value), err
}

func (s *Server) GetVolume(ctx context.Context, request *vmmdv1.GetVolumeRequest) (*vmmdv1.GetVolumeResponse, error) {
	volume, err := s.runtime.GetVolume(ctx, request.GetActorNamespaceId(), request.GetVolumeId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.GetVolumeResponse{Volume: volume}, nil
}

func (s *Server) ListVolumes(ctx context.Context, request *vmmdv1.ListVolumesRequest) (*vmmdv1.ListVolumesResponse, error) {
	volumes, err := s.runtime.ListVolumesBounded(ctx, request.GetActorNamespaceId(), request.GetLimit(), request.GetRelatedContainerLimit())
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
	images, err := s.runtime.ListImagesBounded(ctx, request.GetActorNamespaceId(), request.GetLimit())
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
	if gateErr := s.runtime.ociGrowthError(); gateErr != nil {
		probes.quota = gateErr
	}
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
	diskErr := stateFilesystemsHealth(filesystemHealth)
	_, quotaErr := projectUsage(s.runtime.stateDevice, 0)
	_, containerdErr := s.runtime.client.Version(ctx)
	var egressErr error
	if s.health.EgressHealth != nil {
		egressErr = s.health.EgressHealth()
	}
	return healthProbeResult{
		disk:       diskErr,
		network:    s.runtime.network.Health(),
		egress:     egressErr,
		quota:      quotaErr,
		containerd: containerdErr,
	}
}

func stateFilesystemsHealth(probe func(string) error) error {
	return errors.Join(probe(stateRoot), probe(controlRoot))
}

func filesystemHealth(path string) error {
	var disk syscall.Statfs_t
	if err := syscall.Statfs(path, &disk); err != nil {
		return err
	}
	if disk.Blocks == 0 {
		return fmt.Errorf("%s filesystem reports zero blocks", filepath.Base(path))
	}
	return pathWritable(path)
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
