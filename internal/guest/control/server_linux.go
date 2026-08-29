//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type HealthState struct {
	BootID       string
	RootReadOnly bool
	Versions     map[string]string
	EgressHealth func() error
	EgressStatus func() string
}

type Server struct {
	vmmdv1.UnimplementedVMMDServiceServer
	runtime      *Runtime
	health       HealthState
	dedupe       deduper
	shuttingDown atomic.Bool
	operationMu  sync.Mutex
	operations   sync.WaitGroup
	execMu       sync.Mutex
	executions   map[string]*execRecord
	prepareHost  func(context.Context) error
	healthProbe  func(context.Context) healthProbeResult
	events       eventBuffer
	appliances   *applianceSupervisor
}

func NewServer(runtime *Runtime, health HealthState, prepareHost func(context.Context) error) *Server {
	server := &Server{runtime: runtime, health: health, prepareHost: prepareHost, events: newEventBuffer()}
	server.healthProbe = server.probeComponents
	server.appliances = newApplianceSupervisor(runtime, health.BootID)
	return server
}

func NewGRPCServer() *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(deadlineUnaryInterceptor),
		grpc.ChainStreamInterceptor(deadlineStreamInterceptor),
		grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20),
		grpc.MaxConcurrentStreams(128),
	)
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.beginShutdown(ctx); err != nil {
		return err
	}
	return errors.Join(s.appliances.shutdown(ctx), s.runtime.cleanupTasks(ctx))
}

func (s *Server) Health(context.Context, *vmmdv1.HealthRequest) (*vmmdv1.HealthResponse, error) {
	return &vmmdv1.HealthResponse{Ready: !s.shuttingDown.Load(), StateInitialized: true, RootReadOnly: s.health.RootReadOnly, BootId: s.health.BootID, RuntimeVersions: s.health.Versions}, nil
}

func (s *Server) PrepareShutdown(ctx context.Context, request *vmmdv1.PrepareShutdownRequest) (*vmmdv1.PrepareShutdownResponse, error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, invalid(err)
	}
	if err := s.beginShutdown(ctx); err != nil {
		return nil, rpcError(err)
	}
	_, err := s.mutate(ctx, "prepare", request, func() (any, error) {
		return &vmmdv1.PrepareShutdownResponse{}, s.runtime.cleanupTasks(ctx)
	})
	if err != nil {
		return nil, rpcError(err)
	}
	syscall.Sync()
	if err := s.prepareHost(ctx); err != nil {
		return nil, rpcError(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = syscall.Reboot(syscall.LINUX_REBOOT_CMD_POWER_OFF)
	}()
	return &vmmdv1.PrepareShutdownResponse{}, nil
}

func (s *Server) EnsureNamespace(ctx context.Context, request *vmmdv1.EnsureNamespaceRequest) (*vmmdv1.EnsureNamespaceResponse, error) {
	value, err := s.mutate(ctx, "ensure-namespace", request, func() (any, error) {
		namespace, err := s.runtime.EnsureNamespace(ctx, request)
		return &vmmdv1.EnsureNamespaceResponse{Namespace: namespace}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.EnsureNamespaceResponse), nil
}

func (s *Server) DeleteNamespace(ctx context.Context, request *vmmdv1.DeleteNamespaceRequest) (*vmmdv1.DeleteNamespaceResponse, error) {
	value, err := s.mutate(ctx, "delete-namespace", request, func() (any, error) {
		return &vmmdv1.DeleteNamespaceResponse{}, s.runtime.DeleteNamespace(ctx, request.GetNamespaceId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteNamespaceResponse), nil
}

func (s *Server) PullImage(request *vmmdv1.PullImageRequest, stream grpc.ServerStreamingServer[vmmdv1.PullImageResponse]) error {
	if err := stream.Send(&vmmdv1.PullImageResponse{Status: "resolving"}); err != nil {
		return err
	}
	value, err := s.mutate(stream.Context(), "pull-image", request, func() (any, error) {
		canonical, digest, size, err := s.runtime.PullImage(stream.Context(), request.GetActorNamespaceId(), request.GetReference())
		return &vmmdv1.PullImageResponse{Status: "complete", CanonicalReference: canonical, Digest: &digest, LogicalBytes: size}, err
	})
	if err != nil {
		return rpcError(err)
	}
	return stream.Send(value.(*vmmdv1.PullImageResponse))
}

func (s *Server) DeleteImage(ctx context.Context, request *vmmdv1.DeleteImageRequest) (*vmmdv1.DeleteImageResponse, error) {
	value, err := s.mutate(ctx, "delete-image", request, func() (any, error) {
		return &vmmdv1.DeleteImageResponse{}, s.runtime.DeleteImage(ctx, request.GetActorNamespaceId(), request.GetReference(), request.GetExpectedGenerationId(), request.GetExpectedDigest())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteImageResponse), nil
}

func (s *Server) CreateVolume(ctx context.Context, request *vmmdv1.CreateVolumeRequest) (*vmmdv1.CreateVolumeResponse, error) {
	value, err := s.mutate(ctx, "create-volume", request, func() (any, error) {
		volume, err := s.runtime.CreateVolume(ctx, request.GetActorNamespaceId(), request.GetVolumeId(), request.OwnerUid, request.OwnerGid, request.Mode)
		return &vmmdv1.CreateVolumeResponse{Volume: volume}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.CreateVolumeResponse), nil
}

func (s *Server) DeleteVolume(ctx context.Context, request *vmmdv1.DeleteVolumeRequest) (*vmmdv1.DeleteVolumeResponse, error) {
	value, err := s.mutate(ctx, "delete-volume", request, func() (any, error) {
		return &vmmdv1.DeleteVolumeResponse{}, s.runtime.DeleteVolume(ctx, request.GetActorNamespaceId(), request.GetVolumeId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteVolumeResponse), nil
}

func (s *Server) CreateContainer(ctx context.Context, request *vmmdv1.CreateContainerRequest) (*vmmdv1.CreateContainerResponse, error) {
	value, err := s.mutate(ctx, "create-container", request, func() (any, error) {
		container, err := s.runtime.CreateContainer(ctx, request)
		return &vmmdv1.CreateContainerResponse{Container: container}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.CreateContainerResponse), nil
}

func (s *Server) StartContainer(ctx context.Context, request *vmmdv1.StartContainerRequest) (*vmmdv1.StartContainerResponse, error) {
	value, err := s.mutate(ctx, "start-container", request, func() (any, error) {
		container, err := s.runtime.StartContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
		return &vmmdv1.StartContainerResponse{Container: container}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.StartContainerResponse), nil
}

func (s *Server) StopContainer(ctx context.Context, request *vmmdv1.StopContainerRequest) (*vmmdv1.StopContainerResponse, error) {
	timeout := 10 * time.Second
	if request.TimeoutSeconds != nil {
		if request.GetTimeoutSeconds() > 60 {
			return nil, invalid(errors.New("stop timeout exceeds 60 seconds"))
		}
		timeout = time.Duration(request.GetTimeoutSeconds()) * time.Second
	}
	value, err := s.mutate(ctx, "stop-container", request, func() (any, error) {
		container, err := s.runtime.StopContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId(), timeout)
		return &vmmdv1.StopContainerResponse{Container: container}, err
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.StopContainerResponse), nil
}

func (s *Server) WaitContainer(ctx context.Context, request *vmmdv1.WaitContainerRequest) (*vmmdv1.WaitContainerResponse, error) {
	code, err := s.runtime.WaitContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.WaitContainerResponse{ExitCode: code}, nil
}

func (s *Server) DeleteContainer(ctx context.Context, request *vmmdv1.DeleteContainerRequest) (*vmmdv1.DeleteContainerResponse, error) {
	value, err := s.mutate(ctx, "delete-container", request, func() (any, error) {
		return &vmmdv1.DeleteContainerResponse{}, s.runtime.DeleteContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId(), request.GetExpectedGenerationId())
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.DeleteContainerResponse), nil
}

func (s *Server) InspectContainer(ctx context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	container, err := s.runtime.InspectContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.InspectContainerResponse{Container: container}, nil
}

func (s *Server) ListContainers(ctx context.Context, request *vmmdv1.ListContainersRequest) (*vmmdv1.ListContainersResponse, error) {
	containers, err := s.runtime.ListContainers(ctx, request.GetActorNamespaceId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &vmmdv1.ListContainersResponse{Containers: containers}, nil
}

func (s *Server) Logs(request *vmmdv1.LogsRequest, stream grpc.ServerStreamingServer[vmmdv1.LogsResponse]) error {
	if _, err := s.runtime.InspectContainer(stream.Context(), request.GetActorNamespaceId(), request.GetContainerId()); err != nil {
		return rpcError(err)
	}
	return rpcError(s.runtime.StreamLogs(stream.Context(), request, func(response *vmmdv1.LogsResponse) error { return stream.Send(response) }))
}

func (s *Server) SignalContainer(ctx context.Context, request *vmmdv1.SignalContainerRequest) (*vmmdv1.SignalContainerResponse, error) {
	signal, err := processSignal(request.GetSignal())
	if err != nil {
		return nil, invalid(err)
	}
	value, err := s.mutate(ctx, "signal-container", request, func() (any, error) {
		return &vmmdv1.SignalContainerResponse{}, s.runtime.SignalContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId(), signal)
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return value.(*vmmdv1.SignalContainerResponse), nil
}

func (s *Server) mutate(ctx context.Context, operation string, request proto.Message, function func() (any, error)) (any, error) {
	if operation != "prepare" {
		s.operationMu.Lock()
		if s.shuttingDown.Load() {
			s.operationMu.Unlock()
			return nil, status.Error(codes.Unavailable, "vmmd is shutting down")
		}
		s.operations.Add(1)
		s.operationMu.Unlock()
		defer s.operations.Done()
	}
	requestID := mutationRequestID(request)
	if err := validateRequestID(requestID); err != nil {
		return nil, invalid(err)
	}
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(bytes))
	return s.dedupe.do(ctx, operation+"/"+requestID, fingerprint, func() (any, error) {
		value, err := function()
		if err == nil {
			s.events.publish(&vmmdv1.RuntimeEvent{Type: operation, GuestBootId: s.health.BootID, RequestId: requestID, TimestampUnixNano: time.Now().UnixNano()})
		}
		return value, err
	})
}

func (s *Server) beginShutdown(ctx context.Context) error {
	s.operationMu.Lock()
	s.shuttingDown.Store(true)
	s.operationMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.operations.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func mutationRequestID(request proto.Message) string {
	switch request := request.(type) {
	case *vmmdv1.PrepareShutdownRequest:
		return request.GetRequestId()
	case *vmmdv1.EnsureSystemApplianceRequest:
		return request.GetRequestId()
	case *vmmdv1.StopSystemApplianceRequest:
		return request.GetRequestId()
	case *vmmdv1.EnsureNamespaceRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteNamespaceRequest:
		return request.GetRequestId()
	case *vmmdv1.PullImageRequest:
		return request.GetRequestId()
	case *vmmdv1.ImportImageHeader:
		return request.GetRequestId()
	case *vmmdv1.DeleteImageRequest:
		return request.GetRequestId()
	case *vmmdv1.CreateVolumeRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteVolumeRequest:
		return request.GetRequestId()
	case *vmmdv1.CreateContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.StartContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.StopContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.SignalContainerRequest:
		return request.GetRequestId()
	case *vmmdv1.ApplyNetworkLinkRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteNetworkLinkRequest:
		return request.GetRequestId()
	case *vmmdv1.ApplyServiceProjectionRequest:
		return request.GetRequestId()
	case *vmmdv1.DeleteServiceProjectionRequest:
		return request.GetRequestId()
	default:
		return ""
	}
}

func processSignal(value vmmdv1.ProcessSignal) (syscall.Signal, error) {
	switch value {
	case vmmdv1.ProcessSignal_PROCESS_SIGNAL_TERM:
		return syscall.SIGTERM, nil
	case vmmdv1.ProcessSignal_PROCESS_SIGNAL_KILL:
		return syscall.SIGKILL, nil
	case vmmdv1.ProcessSignal_PROCESS_SIGNAL_INT:
		return syscall.SIGINT, nil
	default:
		return 0, errors.New("signal is required")
	}
}

func deadlineUnaryInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if _, ok := ctx.Deadline(); ok {
		return handler(ctx, request)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return handler(ctx, request)
}

func deadlineStreamInterceptor(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if _, ok := stream.Context().Deadline(); ok {
		return handler(server, stream)
	}
	duration := 30 * time.Minute
	if info.FullMethod == vmmdv1.VMMDService_Exec_FullMethodName || info.FullMethod == vmmdv1.VMMDService_ForwardTCP_FullMethodName || info.FullMethod == vmmdv1.VMMDService_ImportImage_FullMethodName {
		duration = 8 * time.Hour
	}
	ctx, cancel := context.WithTimeout(stream.Context(), duration)
	defer cancel()
	return handler(server, &contextServerStream{ServerStream: stream, ctx: ctx})
}

type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *contextServerStream) Context() context.Context { return stream.ctx }

func invalid(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var stageError runtimeStageError
	if errors.As(err, &stageError) && (status.Code(err) == codes.NotFound || errdefs.IsNotFound(err) || errors.Is(err, os.ErrNotExist)) {
		return detailedErrorMetadata(codes.NotFound, "RUNTIME_RESOURCE_NOT_FOUND",
			"runtime resource was not found during "+stageError.stage, "runtime", "", map[string]string{"stage": stageError.stage})
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	switch {
	case errdefs.IsNotFound(err), errors.Is(err, os.ErrNotExist):
		return status.Error(codes.NotFound, "resource was not found")
	case errdefs.IsAlreadyExists(err):
		return status.Error(codes.AlreadyExists, "resource already exists")
	case errdefs.IsFailedPrecondition(err):
		log.Printf("guest runtime precondition failed: %v", err)
		message := err.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		return detailedError(codes.FailedPrecondition, "RUNTIME_PRECONDITION_FAILED", "runtime precondition failed: "+message, "runtime", "")
	case errors.Is(err, errResourceExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, syscall.EAGAIN) || strings.Contains(strings.ToLower(err.Error()), "resource temporarily unavailable"):
		return detailedError(codes.ResourceExhausted, "PID_LIMIT_EXCEEDED", "environment process limit was exhausted; stop finished execs or containers before retrying", "runtime", "")
	case errors.Is(err, errPrecondition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		log.Printf("guest runtime operation failed: %v", err)
		message := err.Error()
		if len(message) > 512 {
			message = message[:512]
		}
		return detailedError(codes.Internal, "RUNTIME_OPERATION_FAILED", "runtime operation failed: "+message, "runtime", "")
	}
}
