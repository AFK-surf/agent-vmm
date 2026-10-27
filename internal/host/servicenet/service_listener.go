//go:build darwin || linux

package servicenet

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type serviceListenerRuntime struct {
	listener net.Listener

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

func (controller *Owner) ListServiceListeners(ctx context.Context, _ *hostv1.ListServiceListenersRequest) (*hostv1.ListServiceListenersResponse, error) {
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	listeners, err := controller.store.ListServiceListeners(ctx)
	if err != nil {
		return nil, err
	}
	for index, value := range listeners {
		if value.GetLocal() {
			listeners[index] = controller.observeLocalServiceListenerLocked(ctx, value)
		} else {
			listeners[index] = controller.observePersonalServiceListener(value)
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

func (controller *Owner) observePersonalServiceListener(value *hostv1.ServiceListener) *hostv1.ServiceListener {
	observed := proto.Clone(value).(*hostv1.ServiceListener)
	controller.serviceListenerMu.Lock()
	current := controller.serviceListeners[value.GetId()]
	controller.serviceListenerMu.Unlock()
	available := !value.GetRevoked() && value.GetExpiresAt().AsTime().After(time.Now()) && current != nil && current.listening()
	observed.Available = new(available)
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
		observed.Available = new(false)
		observed.UnavailableReason = err.Error()
	}
	return observed
}

func (controller *Owner) RevokeServiceListener(ctx context.Context, request *hostv1.RevokeServiceListenerRequest) (*hostv1.RevokeServiceListenerResponse, error) {
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	if request.GetRequestId() == "" || request.GetListenerId() == "" || request.GetExpectedRevision() == 0 {
		return nil, hostv1.InvalidField("service_listener", "request id, listener id, and expected revision are required")
	}
	if err := controller.store.RevokeServiceListener(ctx, request.GetListenerId(), request.GetExpectedRevision()); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	controller.stopServiceListener(request.GetListenerId())
	return &hostv1.RevokeServiceListenerResponse{}, nil
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

func (controller *Owner) stopServiceListener(id string) {
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

func (controller *Owner) CloseAllServiceListeners() {
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
