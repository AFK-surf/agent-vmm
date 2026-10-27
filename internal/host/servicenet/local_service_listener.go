//go:build darwin || linux

package servicenet

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Owner) CreateLocalServiceListener(ctx context.Context, request *hostv1.CreateLocalServiceListenerRequest) (*hostv1.CreateLocalServiceListenerResponse, error) {
	if request.GetRequestId() == "" || request.GetEnvironmentId() == "" || request.GetContainerId() == "" || request.GetTargetPort() == 0 || request.GetTargetPort() > 65535 || request.GetBindPort() > 65535 {
		return nil, hostv1.InvalidField("service_listener", "request, environment, container and valid target and bind ports are required")
	}
	exposure := request.GetExposure()
	if exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_UNSPECIFIED {
		exposure = hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET
	}
	if exposure != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET && exposure != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK && exposure != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_LAN {
		return nil, hostv1.InvalidField("exposure", "user socket, loopback or LAN exposure is required")
	}
	address := request.GetBindAddress()
	if address == "" && exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK {
		address = "127.0.0.1"
	}
	if exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET {
		if address != "" || request.GetBindPort() != 0 {
			return nil, hostv1.InvalidField("bind_address/bind_port", "user socket exposure does not accept a bind address or port")
		}
	} else {
		parsedAddress, err := netip.ParseAddr(address)
		if err != nil {
			return nil, hostv1.InvalidField("bind_address", "literal IP address is required")
		}
		address = parsedAddress.String()
	}
	unlock, lockErr := controller.serviceLocks.Lock(ctx, "service-listener:"+request.GetRequestId())
	if lockErr != nil {
		return nil, lockErr
	}

	defer unlock()
	if err := controller.projectionMu.Lock(ctx); err != nil {
		return nil, err
	}

	defer controller.projectionMu.Unlock()
	if existing, err := controller.store.GetServiceListenerByRequest(ctx, request.GetRequestId()); err == nil {
		host, port, parseErr := net.SplitHostPort(existing.Listener.GetListenAddress())
		if !existing.Listener.GetLocal() || existing.Listener.GetEnvironmentId() != request.GetEnvironmentId() || existing.Listener.GetContainerId() != request.GetContainerId() || existing.Listener.GetTargetPort() != request.GetTargetPort() || existing.Listener.GetExposure() != exposure || (exposure != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET && (parseErr != nil || host != address || (request.GetBindPort() != 0 && port != strconv.Itoa(int(request.GetBindPort()))))) {
			return nil, status.Error(codes.Aborted, "listener request identifies a different target or bind address")
		}
		return &hostv1.CreateLocalServiceListenerResponse{Listener: controller.observeLocalServiceListenerLocked(ctx, existing.Listener)}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	environment, err := controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	id, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	listener, err := controller.listenLocalService(ctx, id, exposure, address, uint16(request.GetBindPort()))
	if err != nil {
		return nil, err
	}
	value := &hostv1.ServiceListener{Id: id, Local: true, EnvironmentId: environment.ID, ContainerId: request.GetContainerId(), TargetPort: request.GetTargetPort(), Exposure: exposure, ListenAddress: listener.Addr().String(), RouteId: id, Revision: 1}
	created, err := controller.store.CreateServiceListener(ctx, request.GetRequestId(), value, &trustv1.RouteCapability{}, &trustv1.EndpointObservation{})
	if err != nil || created.GetId() != id {
		listener.Close()
		if err != nil {
			return nil, err
		}
		return &hostv1.CreateLocalServiceListenerResponse{Listener: controller.observeLocalServiceListenerLocked(ctx, created)}, nil
	}
	instance, err := controller.localServiceInstance(ctx, created)
	if err != nil || instance == "" {
		listener.Close()
		observed := proto.Clone(created).(*hostv1.ServiceListener)
		observed.Available = new(false)
		observed.UnavailableReason = "service has no current running instance"
		if err != nil {
			observed.UnavailableReason = err.Error()
		}
		return &hostv1.CreateLocalServiceListenerResponse{Listener: observed}, nil
	}
	controller.startLocalServiceListener(listener, created, instance)
	observed := proto.Clone(created).(*hostv1.ServiceListener)
	observed.Available = new(true)
	return &hostv1.CreateLocalServiceListenerResponse{Listener: observed}, nil
}

func (controller *Owner) RestoreLocalServiceListeners(ctx context.Context) error {
	return controller.restoreLocalServiceListenersForContainer(ctx, "", "")
}

func (controller *Owner) RestoreContainerServiceListeners(ctx context.Context, environment, container string) error {
	return controller.restoreLocalServiceListenersForContainer(ctx, environment, container)
}

func (controller *Owner) restoreLocalServiceListenersForContainer(ctx context.Context, environment, container string) error {
	listeners, err := controller.store.ListActiveLocalServiceListeners(ctx)
	if err != nil {
		return err
	}
	active := make(map[string]bool, len(listeners))
	for _, value := range listeners {
		active[value.GetId()] = true
	}
	controller.serviceListenerMu.Lock()
	var stale []string
	for id, runtime := range controller.serviceListeners {
		if runtime.environment != "" && !active[id] && (environment == "" || runtime.environment == environment && runtime.container == container) {
			stale = append(stale, id)
		}
	}
	controller.serviceListenerMu.Unlock()
	for _, id := range stale {
		controller.stopServiceListener(id)
	}
	var failures []error
	for _, value := range listeners {
		if !value.GetLocal() || (environment != "" && (value.GetEnvironmentId() != environment || value.GetContainerId() != container)) {
			continue
		}
		if _, _, err := controller.restoreLocalServiceListenerLocked(ctx, value); err != nil {
			failures = append(failures, fmt.Errorf("restore service listener %s: %w", value.GetId(), err))
		}
	}
	return errors.Join(failures...)
}

func (controller *Owner) restoreLocalServiceListenerLocked(ctx context.Context, value *hostv1.ServiceListener) (bool, string, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	if value.GetRevoked() {
		controller.stopServiceListener(value.GetId())
		return false, "listener is revoked", nil
	}
	instance, err := controller.localServiceInstance(ctx, value)
	if err != nil {
		controller.stopServiceListener(value.GetId())
		return false, "", fmt.Errorf("observe target: %w", err)
	}
	var address string
	var bindPort uint64
	if value.GetExposure() != hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET {
		var port string
		address, port, err = net.SplitHostPort(value.GetListenAddress())
		if err != nil {
			controller.stopServiceListener(value.GetId())
			return false, "", err
		}
		bindPort, err = strconv.ParseUint(port, 10, 16)
		if err != nil {
			controller.stopServiceListener(value.GetId())
			return false, "", err
		}
		if _, err := serviceBindAddress(value.GetExposure(), address); err != nil {
			controller.stopServiceListener(value.GetId())
			if ctx.Err() != nil {
				return false, "", ctx.Err()
			}
			if errors.Is(err, errServiceInterfaceUnavailable) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
	}
	controller.serviceListenerMu.Lock()
	current := controller.serviceListeners[value.GetId()]
	controller.serviceListenerMu.Unlock()
	if instance != "" && current != nil && current.instance == instance && current.listening() {
		return true, "", nil
	}
	controller.stopServiceListener(value.GetId())
	if instance == "" {
		return false, "service has no current running instance", nil
	}
	listener, err := controller.listenLocalService(ctx, value.GetId(), value.GetExposure(), address, uint16(bindPort))
	if err != nil {
		if ctx.Err() != nil {
			return false, "", ctx.Err()
		}
		var socketError *net.OpError
		if errors.Is(err, errServiceInterfaceUnavailable) || errors.As(err, &socketError) && socketError.Op == "listen" {
			return false, err.Error(), nil
		}
		return false, "", err
	}
	controller.startLocalServiceListener(listener, value, instance)
	return true, "", nil
}

func (controller *Owner) observeLocalServiceListenerLocked(ctx context.Context, value *hostv1.ServiceListener) *hostv1.ServiceListener {
	observed := proto.Clone(value).(*hostv1.ServiceListener)
	available, reason, err := controller.restoreLocalServiceListenerLocked(ctx, value)
	observed.Available = new(available)
	observed.UnavailableReason = reason
	if err != nil {
		observed.UnavailableReason = err.Error()
	}
	return observed
}

func (controller *Owner) localServiceInstance(ctx context.Context, value *hostv1.ServiceListener) (string, error) {
	managed, err := controller.store.GetManagedContainer(ctx, value.GetEnvironmentId(), value.GetContainerId())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if err == nil && !managed.DesiredRunning {
		return "", nil
	}
	run, exists := controller.workloads.Instance(value.GetEnvironmentId(), value.GetContainerId())
	if !exists || run.Starting || run.Stopping {
		return "", nil
	}
	return run.InstanceID, nil
}

func (controller *Owner) startLocalServiceListener(listener net.Listener, value *hostv1.ServiceListener, instance string) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &serviceListenerRuntime{listener: listener, cancel: cancel, done: make(chan struct{}), instance: instance, environment: value.GetEnvironmentId(), container: value.GetContainerId()}
	controller.serviceListenerMu.Lock()
	controller.serviceListeners[value.GetId()] = runtime
	controller.serviceListenerMu.Unlock()
	go func() {
		defer close(runtime.done)
		for {
			local, err := listener.Accept()
			if err != nil {
				return
			}
			if !runtime.track(local) {
				local.Close()
				return
			}
			go func() {
				defer runtime.release(local)
				defer local.Close()
				client, err := controller.guestConnection.Client()
				if err == nil {

					err = controller.forwardLocalService(ctx, local, value, instance, client)
				}
				if err != nil {
					controller.logger.Warn("local service connection failed", "listener", value.GetId(), "container_id", value.GetContainerId(), "error", err)
				}
			}()
		}
	}()
}

func (controller *Owner) forwardLocalService(ctx context.Context, local net.Conn, value *hostv1.ServiceListener, instance string, client vmmdv1.VMMDServiceClient) error {
	current, err := controller.store.GetServiceListener(ctx, value.GetId())
	if err != nil {
		return err
	}
	if current.Listener.GetRevoked() {
		return status.Error(codes.Unavailable, "service listener is revoked")
	}
	running, err := controller.localServiceInstance(ctx, value)
	if err != nil {
		return err
	}
	if instance == "" || running != instance {
		return status.Error(codes.Unavailable, "service has no current running instance")
	}
	environment, err := controller.store.GetEnvironment(ctx, value.GetEnvironmentId())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.ForwardTCP(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Header{Header: &vmmdv1.ForwardTCPHeader{
		ActorNamespaceId: environment.NamespaceID, ContainerId: value.GetContainerId(), ContainerInstanceId: instance, Port: value.GetTargetPort(),
	}}}); err != nil {
		return err
	}
	sent := make(chan error, 1)
	go func() {
		buffer := make([]byte, 64*1024)
		for {
			count, err := local.Read(buffer)
			if count != 0 {
				if sendErr := stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_Data{Data: buffer[:count]}}); sendErr != nil {
					sent <- sendErr
					cancel()
					return
				}
			}
			if err == io.EOF {
				err = stream.Send(&vmmdv1.ForwardTCPRequest{Event: &vmmdv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}})
				if err == nil {
					err = stream.CloseSend()
				}
				sent <- err
				return
			}
			if err != nil {
				sent <- err
				cancel()
				return
			}
		}
	}()
	defer func() { local.Close(); cancel(); <-sent }()
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch event := frame.Event.(type) {
		case *vmmdv1.ForwardTCPResponse_Data:
			if _, err := local.Write(event.Data); err != nil {
				return err
			}
		case *vmmdv1.ForwardTCPResponse_CloseRead:
			if socket, ok := local.(interface{ CloseWrite() error }); ok {
				_ = socket.CloseWrite()
			}
		}
	}
}

func (controller *Owner) listenLocalService(ctx context.Context, id string, exposure hostv1.ServiceListenerExposure, address string, port uint16) (net.Listener, error) {
	if exposure == hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_USER_SOCKET {
		digest := sha256.Sum256([]byte(id))
		return privatefs.ListenUnix(filepath.Join(controller.serviceDirectory, fmt.Sprintf("%x.sock", digest[:6])))
	}
	return listenServiceAddress(ctx, exposure, address, port)
}
