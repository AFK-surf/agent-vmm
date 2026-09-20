package remoteconnector

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TunnelMetrics struct {
	AllocationID      string
	LeaseID           string
	BytesToController uint64
	BytesToRuntime    uint64
	SendStalls        uint64
	WindowBlocked     time.Duration
	StartedAt         time.Time
	EndedAt           time.Time
}

type tunnelManager struct {
	ctx                context.Context
	controller         remotev1.RemoteControllerServiceClient
	local              hostv1.RemoteComputeServiceClient
	registrationID     string
	connectionInstance string
	connectionEpoch    uint64
	logger             Logger
	mu                 sync.Mutex
	active             map[string]*activeTunnel
	completed          []TunnelMetrics
	pool               sync.Pool
	bandwidth          *rate.Limiter
}

type activeTunnel struct {
	allocationID string
	leaseID      string
	sessionID    string
	generation   uint64
	cancel       context.CancelFunc
	connection   net.Conn
	ctx          context.Context
	metrics      TunnelMetrics
	toController atomic.Uint64
	toRuntime    atomic.Uint64
	stalls       atomic.Uint64
	blockedNanos atomic.Int64
}

func newTunnelManager(ctx context.Context, controller remotev1.RemoteControllerServiceClient, local hostv1.RemoteComputeServiceClient, registrationID, connectionInstance string, connectionEpoch uint64, logger Logger) *tunnelManager {
	manager := &tunnelManager{ctx: ctx, controller: controller, local: local, registrationID: registrationID, connectionInstance: connectionInstance, connectionEpoch: connectionEpoch, logger: logger, active: make(map[string]*activeTunnel), bandwidth: rate.NewLimiter(rate.Limit(MaxTunnelBytesPerSecond), MaxInFlightBytes)}
	manager.pool.New = func() any { return make([]byte, FrameBytes) }
	return manager
}

func (manager *tunnelManager) Open(ctx context.Context, request *remotev1.OpenSession) (*remotev1.SessionReady, error) {
	if request.GetAllocationId() == "" || request.GetLeaseId() == "" || request.GetGeneration() == 0 || request.GetExecutionOwnerId() == "" || len(request.GetExecutionOwnerId()) > 256 {
		return nil, fmt.Errorf("session identity is invalid")
	}
	manager.mu.Lock()
	if len(manager.active) >= MaxActiveTunnels {
		manager.mu.Unlock()
		return nil, fmt.Errorf("active session limit reached")
	}
	if _, exists := manager.active[request.GetLeaseId()]; exists {
		manager.mu.Unlock()
		return nil, fmt.Errorf("lease already has an active session")
	}
	manager.mu.Unlock()
	opened, err := manager.local.OpenRuntimeSession(ctx, &hostv1.OpenRuntimeSessionRequest{RegistrationId: manager.registrationID, ConnectionInstance: manager.connectionInstance, LeaseId: request.GetLeaseId(), Generation: request.GetGeneration(), AllocationId: request.GetAllocationId(), ExecutionOwnerId: request.GetExecutionOwnerId()})
	if err != nil {
		return nil, err
	}
	revokeOpened := true
	defer func() {
		if revokeOpened {
			manager.revokeSession(opened.GetSessionId(), request.GetLeaseId(), request.GetGeneration())
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
	header := &remotev1.SessionHeader{ProtocolVersion: ProtocolVersion, RegistrationId: manager.registrationID, AllocationId: request.GetAllocationId(), LeaseId: request.GetLeaseId(), Generation: request.GetGeneration(), TunnelNonce: nonce, FrameBytes: FrameBytes, MaxInFlightBytes: MaxInFlightBytes, ConnectionEpoch: manager.connectionEpoch, ExpiresUnixMillis: expiresUnixMillis}
	if err := stream.Send(&remotev1.HostSessionFrame{Payload: &remotev1.HostSessionFrame_Header{Header: header}}); err != nil {
		cancel()
		connection.Close()
		return nil, fmt.Errorf("controller session handshake failed")
	}
	active := &activeTunnel{allocationID: request.GetAllocationId(), leaseID: request.GetLeaseId(), sessionID: opened.GetSessionId(), generation: request.GetGeneration(), cancel: cancel, connection: connection, ctx: tunnelContext, metrics: TunnelMetrics{AllocationID: request.GetAllocationId(), LeaseID: request.GetLeaseId(), StartedAt: time.Now()}}
	manager.mu.Lock()
	if _, exists := manager.active[request.GetLeaseId()]; exists {
		manager.mu.Unlock()
		cancel()
		connection.Close()
		return nil, fmt.Errorf("lease already has an active session")
	}
	manager.active[request.GetLeaseId()] = active
	manager.mu.Unlock()
	revokeOpened = false
	go manager.bridge(active, stream)
	return &remotev1.SessionReady{AllocationId: request.GetAllocationId(), LeaseId: request.GetLeaseId(), Generation: request.GetGeneration(), TunnelNonce: nonce, ExpiresUnixMillis: expiresUnixMillis}, nil
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
		manager.logger.Printf("runtime session bridge failed lease=%s: %v", active.leaseID, first.err)
	}
	if first.err != nil || first.controllerToRuntime {
		active.cancel()
		active.connection.Close()
	}
	second := <-done
	if second.err != nil && manager.logger != nil {
		manager.logger.Printf("runtime session bridge shutdown lease=%s: %v", active.leaseID, second.err)
	}
	if second.err != nil {
		active.cancel()
		active.connection.Close()
	}
	active.cancel()
	active.connection.Close()
	manager.revokeSession(active.sessionID, active.leaseID, active.generation)
	manager.mu.Lock()
	delete(manager.active, active.leaseID)
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
		manager.logger.Printf("runtime session closed lease=%s bytes_out=%d bytes_in=%d stalls=%d", active.leaseID, active.metrics.BytesToController, active.metrics.BytesToRuntime, active.metrics.SendStalls)
	}
}

func (manager *tunnelManager) revokeSession(sessionID, leaseID string, generation uint64) {
	if sessionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := manager.local.RevokeRuntimeSession(ctx, &hostv1.RevokeRuntimeSessionRequest{
		SessionId: sessionID, LeaseId: leaseID, Generation: generation,
		RegistrationId: manager.registrationID, ConnectionInstance: manager.connectionInstance,
	})
	if err != nil && manager.logger != nil {
		manager.logger.Printf("runtime session revoke failed lease=%s session=%s: %v", leaseID, sessionID, err)
	}
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

func (manager *tunnelManager) CloseLease(leaseID string) {
	manager.closeMatching(func(tunnel *activeTunnel) bool { return tunnel.leaseID == leaseID })
}
func (manager *tunnelManager) CloseAllocation(allocationID string) {
	manager.closeMatching(func(tunnel *activeTunnel) bool { return tunnel.allocationID == allocationID })
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
