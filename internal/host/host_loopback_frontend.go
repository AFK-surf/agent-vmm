//go:build darwin || linux

package host

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const hostFrontendPrefaceBytes = 72

type hostLoopbackFrontendManager struct {
	controller *Controller
	mu         sync.Mutex
	leases     map[string]*hostLoopbackFrontendLease
	pending    map[string]chan *hostLoopbackPendingConnection
	events     map[string]chan *hostv1.HostLoopbackFrontendEvent
	preauth    chan struct{}
}

type hostLoopbackFrontendLease struct {
	manager  *hostLoopbackFrontendManager
	proto    *hostv1.HostLoopbackFrontendLease
	token    [32]byte
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc

	mu          sync.Mutex
	connections map[string]net.Conn
	uses        uint32
	closed      bool
	closeOnce   sync.Once
}

type hostLoopbackPendingConnection struct {
	lease        *hostLoopbackFrontendLease
	connectionID string
	connection   net.Conn
	deadline     time.Time
}

func newHostLoopbackFrontendManager(controller *Controller) *hostLoopbackFrontendManager {
	return &hostLoopbackFrontendManager{controller: controller, leases: make(map[string]*hostLoopbackFrontendLease), pending: make(map[string]chan *hostLoopbackPendingConnection),
		events: make(map[string]chan *hostv1.HostLoopbackFrontendEvent), preauth: make(chan struct{}, 256)}
}

func frontendConnectionKey(registrationID, instance string) string {
	return registrationID + "\x00" + instance
}

func frontendEpochKey(registrationID, instance string, epoch uint64) string {
	return frontendConnectionKey(registrationID, instance) + fmt.Sprint("\x00", epoch)
}

func (manager *hostLoopbackFrontendManager) queue(registrationID, instance string, epoch uint64) chan *hostLoopbackPendingConnection {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	key := frontendEpochKey(registrationID, instance, epoch)
	queue := manager.pending[key]
	if queue == nil {
		queue = make(chan *hostLoopbackPendingConnection, 128)
		manager.pending[key] = queue
	}
	return queue
}

func (manager *hostLoopbackFrontendManager) eventQueue(registrationID, instance string, epoch uint64) chan *hostv1.HostLoopbackFrontendEvent {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	key := frontendEpochKey(registrationID, instance, epoch)
	queue := manager.events[key]
	if queue == nil {
		queue = make(chan *hostv1.HostLoopbackFrontendEvent, 128)
		manager.events[key] = queue
	}
	return queue
}

func (manager *hostLoopbackFrontendManager) discardEpoch(registrationID, instance string, epoch uint64) {
	if manager == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	key := frontendEpochKey(registrationID, instance, epoch)
	delete(manager.pending, key)
	delete(manager.events, key)
}

func (manager *hostLoopbackFrontendManager) add(lease *hostLoopbackFrontendLease, capacity *hostv1.HostLoopbackFrontendCapacity) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var total, allocation uint32
	for _, current := range manager.leases {
		if current.proto.GetRegistrationId() != lease.proto.GetRegistrationId() {
			continue
		}
		total++
		if current.proto.GetAllocationId() == lease.proto.GetAllocationId() {
			allocation++
		}
	}
	if len(manager.leases) >= 64 || total >= capacity.GetMaxActiveLeases() || allocation >= capacity.GetMaxActiveLeasesPerAllocation() {
		return status.Error(codes.ResourceExhausted, "host loopback frontend capacity is exhausted")
	}
	manager.leases[lease.proto.GetFrontendLeaseId()] = lease
	return nil
}

func (manager *hostLoopbackFrontendManager) reserveConnection(lease *hostLoopbackFrontendLease, connection net.Conn) (string, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var global, registration uint32
	for _, current := range manager.leases {
		current.mu.Lock()
		count := uint32(len(current.connections))
		current.mu.Unlock()
		global += count
		if current.proto.GetRegistrationId() == lease.proto.GetRegistrationId() {
			registration += count
		}
	}
	capacity := lease.proto.GetEffectiveCapacity()
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || !time.Now().Before(lease.proto.GetExpiresAt().AsTime()) || lease.uses >= capacity.GetMaxUseCount() ||
		uint32(len(lease.connections)) >= capacity.GetMaxConcurrentConnectionsPerLease() || registration >= capacity.GetMaxConcurrentConnections() || global >= 256 {
		return "", false
	}
	connectionID, err := randomID()
	if err != nil {
		return "", false
	}
	lease.uses++
	lease.connections[connectionID] = connection
	return connectionID, true
}

func (manager *hostLoopbackFrontendManager) remove(id string, generation uint64, kind hostv1.HostLoopbackFrontendEvent_Kind) bool {
	manager.mu.Lock()
	lease := manager.leases[id]
	if lease == nil || lease.proto.GetGeneration() != generation {
		manager.mu.Unlock()
		return false
	}
	delete(manager.leases, id)
	manager.mu.Unlock()
	lease.close(kind)
	return true
}

func (manager *hostLoopbackFrontendManager) closeMatching(match func(*hostLoopbackFrontendLease) bool, kind hostv1.HostLoopbackFrontendEvent_Kind) {
	manager.mu.Lock()
	var leases []*hostLoopbackFrontendLease
	for id, lease := range manager.leases {
		if match(lease) {
			delete(manager.leases, id)
			leases = append(leases, lease)
		}
	}
	manager.mu.Unlock()
	for _, lease := range leases {
		lease.close(kind)
	}
}

func (manager *hostLoopbackFrontendManager) closeAll(kind hostv1.HostLoopbackFrontendEvent_Kind) {
	if manager == nil {
		return
	}
	manager.closeMatching(func(*hostLoopbackFrontendLease) bool { return true }, kind)
}

func (manager *hostLoopbackFrontendManager) closeRegistration(registrationID, instance string, kind hostv1.HostLoopbackFrontendEvent_Kind) {
	if manager == nil {
		return
	}
	manager.closeMatching(func(lease *hostLoopbackFrontendLease) bool {
		return lease.proto.GetRegistrationId() == registrationID && (instance == "" || lease.proto.GetConnectionInstance() == instance)
	}, kind)
}

func (manager *hostLoopbackFrontendManager) closeAllocation(registrationID, allocationID string, generation uint64, kind hostv1.HostLoopbackFrontendEvent_Kind) {
	if manager == nil {
		return
	}
	manager.closeMatching(func(lease *hostLoopbackFrontendLease) bool {
		return lease.proto.GetRegistrationId() == registrationID && lease.proto.GetAllocationId() == allocationID && lease.proto.GetAllocationGeneration() == generation
	}, kind)
}

func (lease *hostLoopbackFrontendLease) serve() {
	for {
		connection, err := lease.listener.Accept()
		if err != nil {
			return
		}
		select {
		case lease.manager.preauth <- struct{}{}:
			go func() { defer func() { <-lease.manager.preauth }(); lease.authenticate(connection) }()
		default:
			connection.Close()
		}
	}
}

func (lease *hostLoopbackFrontendLease) drop(connectionID string) {
	lease.mu.Lock()
	connection := lease.connections[connectionID]
	delete(lease.connections, connectionID)
	lease.mu.Unlock()
	if connection != nil {
		connection.Close()
	}
}

func (lease *hostLoopbackFrontendLease) close(kind hostv1.HostLoopbackFrontendEvent_Kind) {
	lease.closeOnce.Do(func() {
		lease.cancel()
		lease.listener.Close()
		lease.mu.Lock()
		lease.closed = true
		connections := make([]net.Conn, 0, len(lease.connections))
		for _, connection := range lease.connections {
			connections = append(connections, connection)
		}
		clear(lease.connections)
		lease.mu.Unlock()
		for _, connection := range connections {
			connection.Close()
		}
		event := &hostv1.HostLoopbackFrontendEvent{Kind: kind, FrontendLeaseId: lease.proto.GetFrontendLeaseId(), Generation: lease.proto.GetGeneration(), AssignmentId: lease.proto.GetAssignmentId()}
		select {
		case lease.manager.eventQueue(lease.proto.GetRegistrationId(), lease.proto.GetConnectionInstance(), lease.proto.GetConnectionEpoch()) <- event:
		default:
		}
	})
}

func (server *remoteComputeServer) AcquireHostLoopbackFrontend(ctx context.Context, request *hostv1.AcquireHostLoopbackFrontendRequest) (*hostv1.HostLoopbackFrontendLease, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	if request.GetAssignmentId() == "" || len(request.GetAssignmentId()) > 256 || request.GetAllocationId() == "" || request.GetAllocationGeneration() == 0 || request.GetTargetRevision() == 0 {
		return nil, invalidField("frontend identity", "exact allocation, generation, revision, and assignment are required")
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	registration, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance())
	if err != nil {
		return nil, err
	}
	if err := server.requireFrontendConnectionEpochLocked(request.GetRegistrationId(), request.GetConnectionInstance(), request.GetConnectionEpoch()); err != nil {
		return nil, err
	}
	if registration.State != int32(hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED) ||
		registration.EnrollmentState != int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY) || registration.OwnerKind != store.RemoteRegistrationPrivate {
		return nil, status.Error(codes.PermissionDenied, "host loopback frontend requires an enabled private controller")
	}
	grant, err := server.controller.store.GetApplianceGrant(ctx, registration.InstallationID)
	if err != nil || grant.DynamicHostLoopback.MaxActiveLeases == 0 {
		return nil, status.Error(codes.PermissionDenied, "host loopback frontend is not granted")
	}
	environment, err := server.remoteEnvironment(ctx, registration.ID, request.GetAllocationId())
	if err != nil {
		return nil, err
	}
	if environment.PolicyRevision != request.GetTargetRevision() || environment.AllocationGeneration != request.GetAllocationGeneration() ||
		environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		return nil, resourceError(codes.Aborted, "POLICY_REVISION_CONFLICT", "allocation", request.GetAllocationId(), "allocation policy revision is stale")
	}
	capacity := hostLoopbackCapacityToProto(grant.DynamicHostLoopback)
	duration := request.GetDuration().AsDuration()
	if duration <= 0 || duration > time.Duration(capacity.GetMaxLeaseSeconds())*time.Second {
		return nil, invalidField("duration", "frontend lease duration exceeds the approved grant")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		listener.Close()
		return nil, err
	}
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		listener.Close()
		return nil, err
	}
	leaseCtx, cancel := context.WithCancel(context.Background())
	expires := time.Now().Add(duration)
	value := &hostv1.HostLoopbackFrontendLease{FrontendLeaseId: id, Generation: server.controller.hostFrontendGeneration.Add(1), RegistrationId: registration.ID,
		ConnectionInstance: request.GetConnectionInstance(), AllocationId: request.GetAllocationId(), AllocationGeneration: request.GetAllocationGeneration(),
		AllocationRevision: request.GetTargetRevision(), AssignmentId: request.GetAssignmentId(),
		Endpoint: listener.Addr().String(), AccessToken: append([]byte(nil), token[:]...), ExpiresAt: timestamppb.New(expires), EffectiveCapacity: capacity, ConnectionEpoch: request.GetConnectionEpoch()}
	lease := &hostLoopbackFrontendLease{manager: server.controller.hostFrontends, proto: value, token: token, listener: listener, ctx: leaseCtx, cancel: cancel, connections: make(map[string]net.Conn)}
	if err := server.controller.hostFrontends.add(lease, capacity); err != nil {
		cancel()
		listener.Close()
		return nil, err
	}
	go lease.serve()
	time.AfterFunc(duration, func() {
		server.controller.hostFrontends.remove(id, value.GetGeneration(), hostv1.HostLoopbackFrontendEvent_KIND_EXPIRED)
	})
	return proto.Clone(value).(*hostv1.HostLoopbackFrontendLease), nil
}

func (server *remoteComputeServer) RenewHostLoopbackFrontend(ctx context.Context, request *hostv1.RenewHostLoopbackFrontendRequest) (*hostv1.HostLoopbackFrontendLease, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return nil, err
	}
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	if err := server.requireFrontendConnectionEpochLocked(request.GetRegistrationId(), request.GetConnectionInstance(), request.GetConnectionEpoch()); err != nil {
		return nil, err
	}
	server.controller.hostFrontends.mu.Lock()
	lease := server.controller.hostFrontends.leases[request.GetFrontendLeaseId()]
	if lease == nil || lease.proto.GetGeneration() != request.GetGeneration() || lease.proto.GetRegistrationId() != request.GetRegistrationId() ||
		lease.proto.GetConnectionInstance() != request.GetConnectionInstance() || lease.proto.GetConnectionEpoch() != request.GetConnectionEpoch() {
		server.controller.hostFrontends.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "frontend lease is not active")
	}
	duration := request.GetDuration().AsDuration()
	if duration <= 0 || duration > time.Duration(lease.proto.GetEffectiveCapacity().GetMaxLeaseSeconds())*time.Second {
		server.controller.hostFrontends.mu.Unlock()
		return nil, invalidField("duration", "frontend lease duration exceeds the immutable grant")
	}
	environment, err := server.remoteEnvironment(ctx, lease.proto.GetRegistrationId(), lease.proto.GetAllocationId())
	if err != nil || environment.AllocationGeneration != lease.proto.GetAllocationGeneration() ||
		environment.PolicyRevision != lease.proto.GetAllocationRevision() || environment.AllocationState != int32(hostv1.AllocationState_ALLOCATION_STATE_READY) {
		server.controller.hostFrontends.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "frontend allocation is not current")
	}
	expires := time.Now().Add(duration)
	lease.proto.ExpiresAt = timestamppb.New(expires)
	result := proto.Clone(lease.proto).(*hostv1.HostLoopbackFrontendLease)
	server.controller.hostFrontends.mu.Unlock()
	time.AfterFunc(duration, func() {
		server.controller.hostFrontends.mu.Lock()
		current := server.controller.hostFrontends.leases[result.GetFrontendLeaseId()]
		due := current != nil && current.proto.GetGeneration() == result.GetGeneration() && !current.proto.GetExpiresAt().AsTime().After(expires)
		server.controller.hostFrontends.mu.Unlock()
		if due {
			server.controller.hostFrontends.remove(result.GetFrontendLeaseId(), result.GetGeneration(), hostv1.HostLoopbackFrontendEvent_KIND_EXPIRED)
		}
	})
	return result, nil
}

func (server *remoteComputeServer) ReleaseHostLoopbackFrontend(ctx context.Context, request *hostv1.ReleaseHostLoopbackFrontendRequest) (*emptypb.Empty, error) {
	if err := authorizeRemoteRegistration(ctx, request.GetRegistrationId()); err != nil {
		return nil, err
	}
	server.controller.remoteAdmissionMu.Lock()
	defer server.controller.remoteAdmissionMu.Unlock()
	if _, err := server.requireCurrentRemoteConnectionLocked(ctx, request.GetRegistrationId(), request.GetConnectionInstance()); err != nil {
		return nil, err
	}
	if err := server.requireFrontendConnectionEpochLocked(request.GetRegistrationId(), request.GetConnectionInstance(), request.GetConnectionEpoch()); err != nil {
		return nil, err
	}
	server.controller.hostFrontends.mu.Lock()
	lease := server.controller.hostFrontends.leases[request.GetFrontendLeaseId()]
	exact := lease != nil && lease.proto.GetGeneration() == request.GetGeneration() && lease.proto.GetRegistrationId() == request.GetRegistrationId() && lease.proto.GetConnectionInstance() == request.GetConnectionInstance() && lease.proto.GetConnectionEpoch() == request.GetConnectionEpoch()
	server.controller.hostFrontends.mu.Unlock()
	if !exact {
		return nil, status.Error(codes.FailedPrecondition, "frontend lease is not active on this connection")
	}
	if !server.controller.hostFrontends.remove(request.GetFrontendLeaseId(), request.GetGeneration(), hostv1.HostLoopbackFrontendEvent_KIND_RELEASED) {
		return nil, status.Error(codes.FailedPrecondition, "frontend lease is not active")
	}
	return &emptypb.Empty{}, nil
}

func (server *remoteComputeServer) AcceptHostLoopbackFrontend(stream hostv1.RemoteComputeService_AcceptHostLoopbackFrontendServer) error {
	if err := server.controller.requireRemoteAdmission(); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	accept := first.GetAccept()
	if accept == nil || accept.GetConnectionInstance() == "" {
		return status.Error(codes.InvalidArgument, "first frontend frame must be the exact connector accept identity")
	}
	if err := authorizeRemoteRegistration(stream.Context(), accept.GetRegistrationId()); err != nil {
		return err
	}
	server.controller.remoteAdmissionMu.Lock()
	if err := server.controller.requireRemoteAdmission(); err != nil {
		server.controller.remoteAdmissionMu.Unlock()
		return err
	}
	_, err = server.requireCurrentRemoteConnectionLocked(stream.Context(), accept.GetRegistrationId(), accept.GetConnectionInstance())
	if err == nil {
		err = server.requireFrontendConnectionEpochLocked(accept.GetRegistrationId(), accept.GetConnectionInstance(), accept.GetConnectionEpoch())
	}
	server.controller.remoteAdmissionMu.Unlock()
	if err != nil {
		return err
	}
	admissionGeneration, admissionOpen := server.controller.vmLifecycle.admissionToken()
	if !admissionOpen {
		return server.controller.requireRemoteAdmission()
	}
	var pending *hostLoopbackPendingConnection
	select {
	case pending = <-server.controller.hostFrontends.queue(accept.GetRegistrationId(), accept.GetConnectionInstance(), accept.GetConnectionEpoch()):
	case event := <-server.controller.hostFrontends.eventQueue(accept.GetRegistrationId(), accept.GetConnectionInstance(), accept.GetConnectionEpoch()):
		return stream.Send(&hostv1.HostFrontendFrame{Payload: &hostv1.HostFrontendFrame_Event{Event: event}})
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	currentGeneration, currentOpen := server.controller.vmLifecycle.admissionToken()
	if !currentOpen || currentGeneration != admissionGeneration {
		pending.connection.Close()
		return server.controller.requireRemoteAdmission()
	}
	defer pending.lease.drop(pending.connectionID)
	if pending.lease.proto.GetConnectionInstance() != accept.GetConnectionInstance() || pending.lease.proto.GetConnectionEpoch() != accept.GetConnectionEpoch() {
		return status.Error(codes.Aborted, "frontend connection belongs to a stale connector")
	}
	pending.lease.mu.Lock()
	closed := pending.lease.closed
	pending.lease.mu.Unlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "frontend lease closed before admission")
	}
	header := &hostv1.HostLoopbackFrontendConnectionHeader{RegistrationId: pending.lease.proto.GetRegistrationId(), ConnectionInstance: pending.lease.proto.GetConnectionInstance(),
		FrontendLeaseId: pending.lease.proto.GetFrontendLeaseId(), FrontendLeaseGeneration: pending.lease.proto.GetGeneration(), AllocationId: pending.lease.proto.GetAllocationId(),
		AllocationGeneration: pending.lease.proto.GetAllocationGeneration(),
		AllocationRevision:   pending.lease.proto.GetAllocationRevision(), AssignmentId: pending.lease.proto.GetAssignmentId(), ConnectionId: pending.connectionID,
		EffectiveCapacity: pending.lease.proto.GetEffectiveCapacity(), ConnectionEpoch: pending.lease.proto.GetConnectionEpoch()}
	if err := stream.Send(&hostv1.HostFrontendFrame{Payload: &hostv1.HostFrontendFrame_Header{Header: header}}); err != nil {
		return err
	}
	ackFrame, err := stream.Recv()
	if err != nil {
		return err
	}
	ack := ackFrame.GetAck()
	if ack == nil || ack.GetConnectionId() != pending.connectionID || !ack.GetAccepted() {
		return status.Error(codes.PermissionDenied, "controller rejected frontend connection")
	}
	pending.lease.mu.Lock()
	closed = pending.lease.closed
	pending.lease.mu.Unlock()
	if closed {
		return status.Error(codes.FailedPrecondition, "frontend lease closed before acknowledgement")
	}
	return bridgeHostFrontend(stream.Context(), pending.connection, stream, pending.lease.proto.GetEffectiveCapacity(), pending.deadline)
}
