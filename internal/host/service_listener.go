//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	hosttrust "github.com/AFK-surf/agent-vmm/internal/host/trust"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type serviceListenerRuntime struct {
	listener    net.Listener
	expires     time.Time
	cancel      context.CancelFunc
	done        chan struct{}
	wg          sync.WaitGroup
	mu          sync.Mutex
	closed      bool
	connections map[net.Conn]struct{}
	instance    string
	environment string
	container   string
}

func (server *operatorServer) CreatePersonalServiceListener(ctx context.Context, request *hostv1.CreatePersonalServiceListenerRequest) (*hostv1.ServiceListenerResponse, error) {
	if request.GetRequestId() == "" || request.GetMeshId() == "" || request.GetDestinationDeviceId() == "" ||
		request.GetDestinationExportId() == "" || request.GetBindPort() > 65535 || request.GetBudget().GetConnectionLimit() == 0 ||
		request.GetBudget().GetConcurrencyLimit() == 0 || request.GetBudget().GetByteLimit() == 0 {
		return nil, invalidField("service_listener", "complete personal service listener and budget are required")
	}
	if existing, err := server.controller.store.GetServiceListenerByRequest(ctx, request.GetRequestId()); err == nil {
		return &hostv1.ServiceListenerResponse{Listener: server.controller.observePersonalServiceListener(existing.Listener)}, nil
	}
	identity, err := server.controller.trust.Identity(ctx)
	if err != nil || identity.Identity.GetDeviceId() == request.GetDestinationDeviceId() {
		return nil, resourceError(codes.FailedPrecondition, "SERVICE_LISTENER_DESTINATION_INVALID", "device", request.GetDestinationDeviceId(), "listener must target another active mesh device")
	}
	endpoint, err := server.controller.trust.GetEndpointObservation(ctx, request.GetDestinationDeviceId())
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "DESTINATION_ENDPOINT_STALE", "device", request.GetDestinationDeviceId(), "a current signed endpoint is required")
	}
	routeClass, routeClassName, err := listenerRouteClass(request.GetExposure())
	if err != nil {
		return nil, invalidField("exposure", err.Error())
	}
	digest := sha256.Sum256([]byte(request.GetRequestId()))
	generation := binary.BigEndian.Uint64(digest[:8])
	if generation == 0 {
		generation = 1
	}
	expires := time.Now().Add(10 * time.Minute)
	unsigned := &servicev1.PersonalRouteRequest{
		RequestId: request.GetRequestId(), MeshId: request.GetMeshId(), DestinationExportId: request.GetDestinationExportId(),
		RouteGeneration: generation, Budget: proto.Clone(request.GetBudget()).(*trustv1.RouteBudget), ExpiresAt: timestamppb.New(expires),
		Nonce: digest[:], RouteClass: routeClass,
	}
	signed, err := server.controller.trust.SignPersonalRouteRequest(ctx, unsigned)
	if err != nil {
		return nil, resourceError(codes.PermissionDenied, "PERSONAL_ROUTE_REQUEST_DENIED", "mesh", request.GetMeshId(), "source mesh membership is not current")
	}
	issued, err := server.controller.requestPersonalRoute(ctx, endpoint, signed)
	if err != nil || issued.GetDestinationExport().GetPort() == 0 || issued.GetCapability().GetRouteClass() != routeClassName {
		return nil, resourceError(codes.Unavailable, "PERSONAL_ROUTE_RESULT_UNKNOWN", "device", request.GetDestinationDeviceId(), "read current service facts before retrying the same request id")
	}
	capability := issued.GetCapability()
	if err := server.controller.trust.AuthorizeRoute(ctx, capability, hosttrust.RouteExpectation{
		SourceDeviceID: identity.Identity.GetDeviceId(), DestinationDeviceID: request.GetDestinationDeviceId(),
		DestinationExportID: request.GetDestinationExportId(), RouteClass: routeClassName, Audience: serviceTunnelAudience,
		RouteGeneration: generation, PolicyRevision: capability.GetPolicyRevision(), ObservedAt: time.Now(),
	}); err != nil {
		return nil, resourceError(codes.PermissionDenied, "ROUTE_CAPABILITY_DENIED", "service_export", request.GetDestinationExportId(), err.Error())
	}
	listener, err := listenServiceAddress(ctx, request.GetExposure(), request.GetBindAddress(), uint16(request.GetBindPort()))
	if err != nil {
		return nil, resourceError(codes.FailedPrecondition, "SERVICE_LISTENER_BIND_FAILED", "address", request.GetBindAddress(), err.Error())
	}
	listenerID, err := randomID()
	if err != nil {
		listener.Close()
		return nil, err
	}
	routeID, err := randomID()
	if err != nil {
		listener.Close()
		return nil, err
	}
	leaseID, err := randomID()
	if err != nil {
		listener.Close()
		return nil, err
	}
	value := &hostv1.ServiceListener{Id: listenerID, MeshId: request.GetMeshId(), DestinationDeviceId: request.GetDestinationDeviceId(),
		DestinationExportId: request.GetDestinationExportId(), Exposure: request.GetExposure(), ListenAddress: listener.Addr().String(),
		RouteId: routeID, CapabilityId: capability.GetCapabilityId(), ExpiresAt: capability.GetExpiresAt(), Revision: 1}
	created, err := server.controller.store.CreateServiceListener(ctx, request.GetRequestId(), value, capability, endpoint)
	if err != nil || created.GetId() != listenerID {
		listener.Close()
		if err == nil {
			return &hostv1.ServiceListenerResponse{Listener: server.controller.observePersonalServiceListener(created)}, nil
		}
		return nil, err
	}
	header := &servicev1.OpenServiceForwardHeader{RouteId: routeID, LeaseId: leaseID, RouteGeneration: generation, Capability: capability}
	server.controller.startServiceListener(listener, created, header, endpoint)
	return &hostv1.ServiceListenerResponse{Listener: server.controller.observePersonalServiceListener(created)}, nil
}

func (server *operatorServer) ListServiceListeners(ctx context.Context, _ *hostv1.ListServiceListenersRequest) (*hostv1.ListServiceListenersResponse, error) {
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	listeners, err := server.controller.store.ListServiceListeners(ctx)
	if err != nil {
		return nil, err
	}
	for index, value := range listeners {
		if value.GetLocal() {
			listeners[index] = server.controller.observeLocalServiceListenerLocked(ctx, value)
		} else {
			listeners[index] = server.controller.observePersonalServiceListener(value)
		}
	}
	return &hostv1.ListServiceListenersResponse{Listeners: listeners}, nil
}

func (runtime *serviceListenerRuntime) listening() bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return false
	}
	select {
	case <-runtime.done:
		return false
	default:
		return true
	}
}

func (controller *Controller) observePersonalServiceListener(value *hostv1.ServiceListener) *hostv1.ServiceListener {
	observed := proto.Clone(value).(*hostv1.ServiceListener)
	controller.serviceListenerMu.Lock()
	current := controller.serviceListeners[value.GetId()]
	controller.serviceListenerMu.Unlock()
	available := !value.GetRevoked() && value.GetExpiresAt().AsTime().After(time.Now()) && current != nil && current.listening()
	observed.Available = optional(available)
	observed.UnavailableReason = ""
	if !available {
		observed.UnavailableReason = "personal listener is not active"
		return observed
	}
	address, _, err := net.SplitHostPort(value.GetListenAddress())
	if err == nil {
		_, err = serviceBindAddress(value.GetExposure(), address)
	}
	if err != nil {
		controller.stopServiceListener(value.GetId())
		observed.Available = optional(false)
		observed.UnavailableReason = err.Error()
	}
	return observed
}

func (server *operatorServer) RevokeServiceListener(ctx context.Context, request *hostv1.RevokeServiceListenerRequest) (*emptypb.Empty, error) {
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	if request.GetRequestId() == "" || request.GetListenerId() == "" || request.GetExpectedRevision() == 0 {
		return nil, invalidField("service_listener", "request id, listener id, and expected revision are required")
	}
	if err := server.controller.store.RevokeServiceListener(ctx, request.GetListenerId(), request.GetExpectedRevision()); err != nil {
		return nil, resourceError(codes.Aborted, "SERVICE_LISTENER_REVISION_CONFLICT", "service_listener", request.GetListenerId(), err.Error())
	}
	server.controller.stopServiceListener(request.GetListenerId())
	return &emptypb.Empty{}, nil
}

func listenerRouteClass(exposure hostv1.ServiceListenerExposure) (servicev1.RouteClass, string, error) {
	switch exposure {
	case hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK:
		return servicev1.RouteClass_ROUTE_CLASS_DEVICE, "device", nil
	case hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN:
		return servicev1.RouteClass_ROUTE_CLASS_LAN, "lan", nil
	default:
		return 0, "", errors.New("device loopback or explicit LAN exposure is required")
	}
}

var errServiceInterfaceUnavailable = errors.New("approved LAN interface address is not currently available")

func serviceBindAddress(exposure hostv1.ServiceListenerExposure, address string) (netip.Addr, error) {
	if address == "" && exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK {
		address = "127.0.0.1"
	}
	parsed, err := netip.ParseAddr(address)
	if err != nil {
		return netip.Addr{}, errors.New("bind address must be a literal IP")
	}
	if exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK {
		if !parsed.IsLoopback() {
			return netip.Addr{}, errors.New("device listener must bind loopback")
		}
	} else {
		if parsed.IsLoopback() || parsed.IsUnspecified() || parsed.IsMulticast() || !parsed.IsPrivate() {
			return netip.Addr{}, errors.New("LAN listener must bind one explicit local private interface address")
		}
		if !localInterfaceContains(parsed) {
			return netip.Addr{}, errServiceInterfaceUnavailable
		}
	}
	return parsed, nil
}

func listenServiceAddress(ctx context.Context, exposure hostv1.ServiceListenerExposure, address string, port uint16) (net.Listener, error) {
	parsed, err := serviceBindAddress(exposure, address)
	if err != nil {
		return nil, err
	}
	var listenConfig net.ListenConfig
	return listenConfig.Listen(ctx, "tcp", net.JoinHostPort(parsed.String(), strconv.Itoa(int(port))))
}

func localInterfaceContains(expected netip.Addr) bool {
	interfaces, err := net.Interfaces()
	if err != nil {
		return false
	}
	for _, current := range interfaces {
		addresses, _ := current.Addrs()
		for _, address := range addresses {
			prefix, parseErr := netip.ParsePrefix(address.String())
			if parseErr == nil && prefix.Addr().Unmap() == expected.Unmap() {
				return true
			}
		}
	}
	return false
}

func (controller *Controller) startServiceListener(listener net.Listener, value *hostv1.ServiceListener, header *servicev1.OpenServiceForwardHeader, endpoint *trustv1.EndpointObservation) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &serviceListenerRuntime{listener: listener, expires: value.GetExpiresAt().AsTime(), cancel: cancel, done: make(chan struct{})}
	controller.serviceListenerMu.Lock()
	controller.serviceListeners[value.GetId()] = runtime
	controller.serviceListenerMu.Unlock()
	go func() {
		defer close(runtime.done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			if time.Now().After(runtime.expires) {
				connection.Close()
				controller.stopServiceListener(value.GetId())
				return
			}
			if !runtime.track(connection) {
				connection.Close()
				return
			}
			go func() {
				defer runtime.release(connection)
				defer connection.Close()
				remote, err := controller.dialRemoteService(ctx, header, endpoint)
				if err != nil {
					return
				}
				defer remote.Close()
				bridgeServiceConnections(connection, remote)
			}()
		}
	}()
	go func() {
		timer := time.NewTimer(time.Until(runtime.expires))
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			controller.stopServiceListener(value.GetId())
			_ = controller.store.RevokeServiceListener(context.Background(), value.GetId(), value.GetRevision())
		}
	}()
}

func bridgeServiceConnections(left, right net.Conn) {
	done := make(chan struct{}, 2)
	copyOne := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		if closer, ok := destination.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyOne(left, right)
	go copyOne(right, left)
	<-done
	<-done
}

func (controller *Controller) stopServiceListener(id string) {
	controller.serviceListenerMu.Lock()
	runtime := controller.serviceListeners[id]
	delete(controller.serviceListeners, id)
	controller.serviceListenerMu.Unlock()
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	runtime.closed = true
	connections := make([]net.Conn, 0, len(runtime.connections))
	for connection := range runtime.connections {
		connections = append(connections, connection)
	}
	runtime.mu.Unlock()
	runtime.cancel()
	_ = runtime.listener.Close()
	for _, connection := range connections {
		_ = connection.Close()
	}
	runtime.wg.Wait()
}

func (controller *Controller) closeAllServiceListeners() {
	controller.serviceListenerMu.Lock()
	ids := make([]string, 0, len(controller.serviceListeners))
	for id := range controller.serviceListeners {
		ids = append(ids, id)
	}
	controller.serviceListenerMu.Unlock()
	for _, id := range ids {
		controller.stopServiceListener(id)
	}
}

func (runtime *serviceListenerRuntime) track(connection net.Conn) bool {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return false
	}
	if runtime.connections == nil {
		runtime.connections = make(map[net.Conn]struct{})
	}
	runtime.connections[connection] = struct{}{}
	runtime.wg.Add(1)
	return true
}

func (runtime *serviceListenerRuntime) release(connection net.Conn) {
	runtime.mu.Lock()
	delete(runtime.connections, connection)
	runtime.mu.Unlock()
	runtime.wg.Done()
}
