package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TunnelMetrics struct {
	AllocationID         string
	AllocationGeneration uint64
	BytesToController    uint64
	BytesToRuntime       uint64
	SendStalls           uint64
	WindowBlocked        time.Duration
	StartedAt            time.Time
	EndedAt              time.Time
}

type tunnelManager struct {
	ctx                context.Context
	controller         remotev1.RemoteControllerServiceClient
	local              hostv1.RemoteComputeServiceClient
	registrationID     string
	connectionInstance string
	connectionEpoch    uint64
	logger             Logger
	openGate           chan struct{}
	mu                 sync.Mutex
	active             map[string]*activeTunnel
	completed          []TunnelMetrics
	pool               sync.Pool
	bandwidth          *rate.Limiter
}

type activeTunnel struct {
	allocationID         string
	sessionID            string
	allocationGeneration uint64
	cancel               context.CancelFunc
	connection           net.Conn
	ctx                  context.Context
	revokeGate           chan struct{}
	revoked              bool
	metrics              TunnelMetrics
	toController         atomic.Uint64
	toRuntime            atomic.Uint64
	stalls               atomic.Uint64
	blockedNanos         atomic.Int64
}

func newTunnelManager(ctx context.Context, controller remotev1.RemoteControllerServiceClient, local hostv1.RemoteComputeServiceClient, registrationID, connectionInstance string, connectionEpoch uint64, logger Logger) *tunnelManager {
	manager := &tunnelManager{ctx: ctx, controller: controller, local: local, registrationID: registrationID, connectionInstance: connectionInstance, connectionEpoch: connectionEpoch, logger: logger, active: make(map[string]*activeTunnel), openGate: make(chan struct{}, 1), bandwidth: rate.NewLimiter(rate.Limit(MaxTunnelBytesPerSecond), MaxInFlightBytes)}
	manager.pool.New = func() any { return make([]byte, FrameBytes) }
	return manager
}

func (manager *tunnelManager) Open(ctx context.Context, request *remotev1.OpenSession) (*remotev1.SessionReady, error) {
	if request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 || request.GetExecutionOwnerId() == "" || len(request.GetExecutionOwnerId()) > 256 {
		return nil, &localRejection{reason: remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, message: "session identity is invalid"}
	}
	select {
	case manager.openGate <- struct{}{}:
		defer func() { <-manager.openGate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-manager.ctx.Done():
		return nil, manager.ctx.Err()
	}
	ctx, cancelOpen := context.WithCancel(ctx)
	stop := context.AfterFunc(manager.ctx, cancelOpen)
	defer stop()
	defer cancelOpen()
	tunnelKey := allocationTunnelKey(request.GetAllocationId(), request.GetAllocationGeneration())
	manager.mu.Lock()
	previous := manager.active[tunnelKey]
	atLimit := previous == nil && len(manager.active) >= MaxActiveTunnels
	manager.mu.Unlock()
	if atLimit {
		return nil, &localRejection{reason: remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED, message: "active session limit reached"}
	}
	if previous != nil {
		previous.cancel()
		previous.connection.Close()
		if err := manager.revokeTunnel(ctx, previous); err != nil {
			return nil, fmt.Errorf("previous runtime session revocation failed: %w", err)
		}
	}
	opened, err := manager.local.OpenRuntimeSession(ctx, &hostv1.OpenRuntimeSessionRequest{RegistrationId: manager.registrationID, ConnectionInstance: manager.connectionInstance, AllocationGeneration: request.GetAllocationGeneration(), AllocationId: request.GetAllocationId(), ExecutionOwnerId: request.GetExecutionOwnerId()})
	if err != nil {
		return nil, err
	}
	revokeOpened := true
	defer func() {
		if revokeOpened {
			manager.revokeSession(opened.GetSessionId(), request.GetAllocationId(), request.GetAllocationGeneration())
		}
	}()
	if err := validateRuntimeSocket(opened.GetSocketPath()); err != nil {
		return nil, err
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", opened.GetSocketPath())
	if err != nil {
		return nil, fmt.Errorf("runtime session unavailable")
	}
	tunnelContext, cancel := context.WithCancel(manager.ctx)
	stopHandshake := context.AfterFunc(ctx, cancel)
	defer stopHandshake()
	stream, err := manager.controller.ServeSession(tunnelContext)
	if err != nil {
		cancel()
		connection.Close()
		return nil, fmt.Errorf("controller session unavailable")
	}
	nonce, err := randomInstance()
	if err != nil {
		cancel()
		connection.Close()
		return nil, err
	}
	expiresUnixMillis := opened.GetExpiresAt().AsTime().UnixMilli()
	header := &remotev1.SessionHeader{ProtocolVersion: ProtocolVersion, RegistrationId: manager.registrationID, AllocationId: request.GetAllocationId(), AllocationGeneration: request.GetAllocationGeneration(), TunnelNonce: nonce, FrameBytes: FrameBytes, MaxInFlightBytes: MaxInFlightBytes, ConnectionEpoch: manager.connectionEpoch, ExpiresUnixMillis: expiresUnixMillis}
	if err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Header{Header: header}}); err != nil {
		cancel()
		connection.Close()
		return nil, fmt.Errorf("controller session handshake failed")
	}
	stopHandshake()
	if err := ctx.Err(); err != nil {
		cancel()
		connection.Close()
		return nil, err
	}
	active := &activeTunnel{revokeGate: make(chan struct{}, 1), allocationID: request.GetAllocationId(), sessionID: opened.GetSessionId(), allocationGeneration: request.GetAllocationGeneration(), cancel: cancel, connection: connection, ctx: tunnelContext, metrics: TunnelMetrics{AllocationID: request.GetAllocationId(), AllocationGeneration: request.GetAllocationGeneration(), StartedAt: time.Now()}}
	manager.mu.Lock()
	manager.active[tunnelKey] = active
	manager.mu.Unlock()
	revokeOpened = false
	go manager.bridge(active, stream)
	return &remotev1.SessionReady{AllocationId: request.GetAllocationId(), AllocationGeneration: request.GetAllocationGeneration(), TunnelNonce: nonce, ExpiresUnixMillis: expiresUnixMillis}, nil
}

func (manager *tunnelManager) bridge(active *activeTunnel, stream remotev1.RemoteControllerService_ServeSessionClient) {
	type bridgeResult struct {
		controllerToRuntime bool
		err                 error
	}
	done := make(chan bridgeResult, 2)
	go func() { done <- bridgeResult{err: manager.runtimeToController(active, stream)} }()
	go func() {
		done <- bridgeResult{controllerToRuntime: true, err: manager.controllerToRuntime(active, stream)}
	}()
	first := <-done
	if first.err != nil && manager.logger != nil {
		manager.logger.Printf("runtime session bridge failed allocation=%s generation=%d: %v", active.allocationID, active.allocationGeneration, first.err)
	}
	if first.err != nil || first.controllerToRuntime {
		active.cancel()
		active.connection.Close()
	}
	second := <-done
	if second.err != nil && manager.logger != nil {
		manager.logger.Printf("runtime session bridge shutdown allocation=%s generation=%d: %v", active.allocationID, active.allocationGeneration, second.err)
	}
	if second.err != nil {
		active.cancel()
		active.connection.Close()
	}
	active.cancel()
	active.connection.Close()
	revokeErr := manager.revokeTunnel(context.Background(), active)
	manager.mu.Lock()
	key := allocationTunnelKey(active.allocationID, active.allocationGeneration)
	if manager.active[key] == active && revokeErr == nil {
		delete(manager.active, key)
	}
	active.metrics.BytesToController = active.toController.Load()
	active.metrics.BytesToRuntime = active.toRuntime.Load()
	active.metrics.SendStalls = active.stalls.Load()
	active.metrics.WindowBlocked = time.Duration(active.blockedNanos.Load())
	active.metrics.EndedAt = time.Now()
	manager.completed = append(manager.completed, active.metrics)
	if len(manager.completed) > MaxActiveTunnels {
		manager.completed = append([]TunnelMetrics(nil), manager.completed[len(manager.completed)-MaxActiveTunnels:]...)
	}
	manager.mu.Unlock()
	if manager.logger != nil {
		manager.logger.Printf("runtime session closed allocation=%s generation=%d bytes_out=%d bytes_in=%d stalls=%d", active.allocationID, active.allocationGeneration, active.metrics.BytesToController, active.metrics.BytesToRuntime, active.metrics.SendStalls)
	}
}

func (manager *tunnelManager) revokeTunnel(ctx context.Context, active *activeTunnel) error {
	select {
	case active.revokeGate <- struct{}{}:
		defer func() { <-active.revokeGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if active.revoked {
		return nil
	}
	err := manager.revokeSessionContext(ctx, active.sessionID, active.allocationID, active.allocationGeneration)
	active.revoked = err == nil
	return err
}

func (manager *tunnelManager) revokeSession(sessionID, allocationID string, allocationGeneration uint64) {
	_ = manager.revokeSessionContext(context.Background(), sessionID, allocationID, allocationGeneration)
}

func (manager *tunnelManager) revokeSessionContext(parent context.Context, sessionID, allocationID string, allocationGeneration uint64) error {
	if sessionID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	_, err := manager.local.RevokeRuntimeSession(ctx, &hostv1.RevokeRuntimeSessionRequest{
		SessionId: sessionID, AllocationId: allocationID, AllocationGeneration: allocationGeneration,
		RegistrationId: manager.registrationID, ConnectionInstance: manager.connectionInstance,
	})
	if status.Code(err) == codes.Aborted {
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == "STALE_RUNTIME_SESSION" {
				return nil
			}
		}
	}
	if err != nil {
		log.Printf("runtime session revoke failed: registration=%s allocation=%s generation=%d session=%s error=%v details=%v", manager.registrationID, allocationID, allocationGeneration, sessionID, err, status.Convert(err).Details())
	}
	return err
}

func (manager *tunnelManager) runtimeToController(active *activeTunnel, stream remotev1.RemoteControllerService_ServeSessionClient) error {
	buffer := manager.pool.Get().([]byte)
	defer manager.pool.Put(buffer)
	for {
		count, err := active.connection.Read(buffer)
		if count > 0 {
			if err := manager.bandwidth.WaitN(active.ctx, count); err != nil {
				return err
			}
			payload := append([]byte(nil), buffer[:count]...)
			started := time.Now()
			sendErr := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Data{Data: payload}})
			blocked := time.Since(started)
			if blocked > time.Millisecond {
				active.stalls.Add(1)
				active.blockedNanos.Add(blocked.Nanoseconds())
			}
			if sendErr != nil {
				return sendErr
			}
			active.toController.Add(uint64(count))
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_CloseWrite{CloseWrite: true}})
				return stream.CloseSend()
			}
			return err
		}
	}
}

func (manager *tunnelManager) controllerToRuntime(active *activeTunnel, stream remotev1.RemoteControllerService_ServeSessionClient) error {
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if data := frame.GetData(); len(data) != 0 {
			if len(data) > FrameBytes {
				return fmt.Errorf("controller frame exceeds limit")
			}
			if err := manager.bandwidth.WaitN(active.ctx, len(data)); err != nil {
				return err
			}
			if err := writeAll(active.connection, data); err != nil {
				return err
			}
			active.toRuntime.Add(uint64(len(data)))
		}
		if frame.GetCloseWrite() {
			if connection, ok := active.connection.(interface{ CloseWrite() error }); ok {
				if err := connection.CloseWrite(); err != nil {
					return err
				}
			}
		}
	}
}

func writeAll(writer io.Writer, value []byte) error {
	for len(value) != 0 {
		count, err := writer.Write(value)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		value = value[count:]
	}
	return nil
}

func (manager *tunnelManager) CloseAllocation(allocationID string) {
	manager.closeMatching(func(tunnel *activeTunnel) bool { return tunnel.allocationID == allocationID })
}

func allocationTunnelKey(allocationID string, allocationGeneration uint64) string {
	return fmt.Sprintf("%s\x00%d", allocationID, allocationGeneration)
}
func (manager *tunnelManager) Close() {
	manager.closeMatching(func(*activeTunnel) bool { return true })
}

func (manager *tunnelManager) Metrics() (int, []TunnelMetrics) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	completed := append([]TunnelMetrics(nil), manager.completed...)
	return len(manager.active), completed
}

func (manager *tunnelManager) DomainMetrics() *hostv1.ControllerDomainMetrics {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result := &hostv1.ControllerDomainMetrics{ActiveSessions: uint32(len(manager.active)), SampledAt: timestamppb.Now()}
	for _, completed := range manager.completed {
		result.BytesToController += completed.BytesToController
		result.BytesToRuntime += completed.BytesToRuntime
		result.SendStalls += completed.SendStalls
		result.WindowBlockedMillis += uint64(completed.WindowBlocked / time.Millisecond)
	}
	for _, active := range manager.active {
		result.BytesToController += active.toController.Load()
		result.BytesToRuntime += active.toRuntime.Load()
		result.SendStalls += active.stalls.Load()
		result.WindowBlockedMillis += uint64(time.Duration(active.blockedNanos.Load()) / time.Millisecond)
	}
	return result
}
func (manager *tunnelManager) closeMatching(matches func(*activeTunnel) bool) {
	manager.mu.Lock()
	tunnels := make([]*activeTunnel, 0, len(manager.active))
	for _, tunnel := range manager.active {
		if matches(tunnel) {
			tunnels = append(tunnels, tunnel)
		}
	}
	manager.mu.Unlock()
	for _, tunnel := range tunnels {
		tunnel.cancel()
		tunnel.connection.Close()
	}
}

func validateRuntimeSocket(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("runtime session socket must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("runtime session socket unavailable")
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("runtime session socket has unsafe permissions")
	}
	if runtime.GOOS != "windows" {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != os.Getuid() {
			return fmt.Errorf("runtime session socket has unsafe owner")
		}
	}
	return nil
}
