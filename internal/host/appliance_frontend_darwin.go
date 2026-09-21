//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

var applianceFrontendID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

type applianceFrontendRuntime struct {
	controller        *Controller
	installationID    string
	name              string
	containerInstance string
	endpoint          string
	endpointFile      string
	frontend          store.ApplianceFrontend
	identity          *vmmdv1.SystemApplianceIdentity
	listener          net.Listener
	open              func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error)
	ctx               context.Context
	cancel            context.CancelFunc

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	uses        uint32
	available   bool
	closeOnce   sync.Once
}

func applianceFrontendEndpointPath(supportDirectory, installationID, frontendName string) string {
	return filepath.Join(supportDirectory, "appliance-frontends", installationID, frontendName+".endpoint")
}

// The endpoint file describes the currently listening socket and is removed
// when that socket is closed. Keep the selected loopback port separately so a
// host restart does not invalidate a client configuration that points at the
// previous endpoint.
func applianceFrontendPortPath(supportDirectory, installationID, frontendName string) string {
	return filepath.Join(supportDirectory, "appliance-frontend-ports", installationID, frontendName+".port")
}

func (controller *Controller) startApplianceFrontends(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity) error {
	if identity == nil || identity.GetInstallationId() != installation.ID || identity.GetContainerInstanceId() == "" {
		return errors.New("frontend identity does not match the appliance")
	}
	frontends, err := controller.store.ListApplianceFrontends(ctx, installation.ID)
	if err != nil {
		return err
	}
	if controller.applianceFrontendsMatch(installation.ID, identity, frontends) {
		return nil
	}
	controller.stopApplianceFrontends(installation.ID)
	runtimes := make([]*applianceFrontendRuntime, 0, len(frontends))
	closeCreated := func() {
		for _, created := range runtimes {
			created.close()
		}
	}
	for _, frontend := range frontends {
		if frontend.Exposure == int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY) {
			continue
		}
		if frontend.Exposure != int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK) ||
			(frontend.Transport != int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM) && frontend.Transport != int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP)) {
			closeCreated()
			return fmt.Errorf("frontend %q has an unsupported projection", frontend.Name)
		}
		runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(streamCtx context.Context, header *vmmdv1.SystemStreamHeader) (net.Conn, error) {
			client, connection, err := controller.guestClient(streamCtx)
			if err != nil {
				return nil, err
			}
			stream, err := openSystemStreamConn(streamCtx, client, header)
			if err != nil {
				connection.Close()
				return nil, err
			}
			return &applianceFrontendGuestConn{Conn: stream, closeConnection: connection.Close}, nil
		})
		if err != nil {
			closeCreated()
			return err
		}
		runtimes = append(runtimes, runtime)
	}
	controller.applianceFrontendMu.Lock()
	for _, runtime := range runtimes {
		controller.applianceFrontends[frontendRuntimeKey(runtime.installationID, runtime.name)] = runtime
	}
	controller.applianceFrontendMu.Unlock()
	for _, runtime := range runtimes {
		go runtime.serve()
	}
	return nil
}

func (controller *Controller) applianceFrontendsMatch(installationID string, identity *vmmdv1.SystemApplianceIdentity, frontends []store.ApplianceFrontend) bool {
	controller.applianceFrontendMu.Lock()
	defer controller.applianceFrontendMu.Unlock()
	if identity == nil {
		return false
	}
	expected := 0
	for _, frontend := range frontends {
		if frontend.Exposure == int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY) {
			continue
		}
		expected++
		runtime := controller.applianceFrontends[frontendRuntimeKey(installationID, frontend.Name)]
		if runtime == nil || runtime.containerInstance != identity.GetContainerInstanceId() || runtime.identity.GetBootId() != identity.GetBootId() {
			return false
		}
	}
	actual := 0
	for _, runtime := range controller.applianceFrontends {
		if runtime.installationID == installationID {
			actual++
		}
	}
	return actual == expected
}

type applianceFrontendGuestConn struct {
	net.Conn
	closeConnection func() error
	closeOnce       sync.Once
}

func (connection *applianceFrontendGuestConn) Close() error {
	var result error
	connection.closeOnce.Do(func() {
		result = errors.Join(connection.Conn.Close(), connection.closeConnection())
	})
	return result
}

func (controller *Controller) newApplianceFrontendRuntime(frontend store.ApplianceFrontend, identity *vmmdv1.SystemApplianceIdentity, open func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error)) (*applianceFrontendRuntime, error) {
	if !applianceFrontendID.MatchString(identity.GetInstallationId()) || !applianceFrontendID.MatchString(frontend.Name) {
		return nil, errors.New("frontend projection identity is invalid")
	}
	if frontend.MaxFrameBytes == 0 || frontend.MaxFrameBytes > 1<<20 || frontend.MaxTotalBytes < uint64(frontend.MaxFrameBytes) || frontend.MaxTotalBytes > 1<<30 ||
		frontend.DeadlineSeconds == 0 || frontend.DeadlineSeconds > 24*60*60 || frontend.IdleSeconds == 0 || frontend.IdleSeconds > 300 ||
		frontend.MaxConcurrentStreams == 0 || frontend.MaxConcurrentStreams > 16 || frontend.MaxUseCount == 0 || frontend.MaxUseCount > 1024 {
		return nil, fmt.Errorf("frontend %q has incomplete stream bounds", frontend.Name)
	}
	portPath := applianceFrontendPortPath(controller.supportDirectory, identity.GetInstallationId(), frontend.Name)
	listenAddress := "127.0.0.1:0"
	if persisted, err := os.ReadFile(portPath); err == nil {
		if value := strings.TrimSpace(string(persisted)); value != "" {
			if host, _, splitErr := net.SplitHostPort(value); splitErr == nil {
				if host == "127.0.0.1" {
					listenAddress = value
				}
			} else if port, parseErr := strconv.Atoi(value); parseErr == nil && port > 0 && port < 1<<16 {
				listenAddress = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
			}
		}
	}
	listener, err := net.Listen("tcp4", listenAddress)
	if err != nil && listenAddress != "127.0.0.1:0" {
		listener, err = net.Listen("tcp4", "127.0.0.1:0")
	}
	if err != nil {
		return nil, err
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	runtime := &applianceFrontendRuntime{controller: controller, installationID: identity.GetInstallationId(), name: frontend.Name,
		containerInstance: identity.GetContainerInstanceId(), endpoint: listener.Addr().String(),
		endpointFile: applianceFrontendEndpointPath(controller.supportDirectory, identity.GetInstallationId(), frontend.Name),
		frontend:     frontend, identity: proto.Clone(identity).(*vmmdv1.SystemApplianceIdentity), listener: listener,
		open: open, ctx: runtimeCtx, cancel: cancel, connections: make(map[net.Conn]struct{}), available: true}
	if err := writeApplianceFrontendPort(portPath, runtime.endpoint); err != nil {
		cancel()
		listener.Close()
		return nil, err
	}
	if err := writeApplianceFrontendEndpoint(runtime.endpointFile, runtime.endpoint); err != nil {
		cancel()
		listener.Close()
		return nil, err
	}
	return runtime, nil
}

func frontendRuntimeKey(installationID, name string) string { return installationID + "/" + name }

func (controller *Controller) stopApplianceFrontends(installationID string) {
	if controller.store != nil {
		if installation, err := controller.store.GetApplianceInstallation(context.Background(), installationID); err == nil && installation.RegistrationID != "" {
			controller.hostFrontends.closeRegistration(installation.RegistrationID, "", hostv1.HostLoopbackFrontendEvent_KIND_REVOKED)
		}
	}
	controller.applianceFrontendMu.Lock()
	var runtimes []*applianceFrontendRuntime
	for key, runtime := range controller.applianceFrontends {
		if runtime.installationID == installationID {
			runtimes = append(runtimes, runtime)
			delete(controller.applianceFrontends, key)
		}
	}
	controller.applianceFrontendMu.Unlock()
	for _, runtime := range runtimes {
		runtime.close()
	}
}

func (controller *Controller) stopAllApplianceFrontends() {
	controller.applianceFrontendMu.Lock()
	runtimes := make([]*applianceFrontendRuntime, 0, len(controller.applianceFrontends))
	for _, runtime := range controller.applianceFrontends {
		runtimes = append(runtimes, runtime)
	}
	clear(controller.applianceFrontends)
	controller.applianceFrontendMu.Unlock()
	for _, runtime := range runtimes {
		runtime.close()
	}
}

func (controller *Controller) applianceFrontendProjection(installationID, name, containerInstance string) (string, bool) {
	controller.applianceFrontendMu.Lock()
	runtime := controller.applianceFrontends[frontendRuntimeKey(installationID, name)]
	controller.applianceFrontendMu.Unlock()
	if runtime == nil || runtime.containerInstance != containerInstance {
		return "", false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.endpoint, runtime.available
}

func (runtime *applianceFrontendRuntime) serve() {
	for {
		connection, err := runtime.listener.Accept()
		if err != nil {
			return
		}
		checkContext, cancel := context.WithTimeout(runtime.ctx, time.Second)
		current := runtime.controller.applianceFrontendInstanceCurrent(checkContext, runtime.identity)
		cancel()
		if !current {
			connection.Close()
			continue
		}
		runtime.mu.Lock()
		if !runtime.available || runtime.uses >= runtime.frontend.MaxUseCount || uint32(len(runtime.connections)) >= runtime.frontend.MaxConcurrentStreams {
			runtime.mu.Unlock()
			connection.Close()
			continue
		}
		runtime.uses++
		runtime.connections[connection] = struct{}{}
		exhausted := runtime.uses == runtime.frontend.MaxUseCount
		if exhausted {
			runtime.available = false
		}
		runtime.mu.Unlock()
		if exhausted {
			runtime.listener.Close()
			_ = os.Remove(runtime.endpointFile)
		}
		go runtime.proxy(connection)
	}
}

func (controller *Controller) applianceFrontendInstanceCurrent(ctx context.Context, identity *vmmdv1.SystemApplianceIdentity) bool {
	if controller == nil || controller.store == nil {
		return true
	}
	if identity == nil || identity.GetInstallationId() == "" || identity.GetContainerInstanceId() == "" {
		return false
	}
	installation, err := controller.store.GetApplianceInstallation(ctx, identity.GetInstallationId())
	if err != nil || installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) ||
		installation.CurrentBootID != identity.GetBootId() || installation.CurrentContainerInstance != identity.GetContainerInstanceId() {
		return false
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return false
	}
	defer connection.Close()
	appliance, err := getSystemAppliance(ctx, client, identity.GetInstallationId())
	if err != nil {
		return false
	}
	if appliance == nil {
		return false
	}
	observed := appliance.GetIdentity()
	return observed.GetBootId() == identity.GetBootId() && observed.GetContainerInstanceId() == identity.GetContainerInstanceId() &&
		appliance.GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING
}

func (runtime *applianceFrontendRuntime) proxy(local net.Conn) {
	defer func() {
		local.Close()
		runtime.mu.Lock()
		delete(runtime.connections, local)
		runtime.mu.Unlock()
	}()
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(runtime.ctx, time.Duration(runtime.frontend.DeadlineSeconds)*time.Second)
	defer cancel()
	remote, err := runtime.open(ctx, &vmmdv1.SystemStreamHeader{Identity: proto.Clone(runtime.identity).(*vmmdv1.SystemApplianceIdentity),
		Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND, Name: runtime.name, Nonce: nonce,
		MaxFrameBytes: runtime.frontend.MaxFrameBytes, MaxTotalBytes: runtime.frontend.MaxTotalBytes,
		IdleSeconds: runtime.frontend.IdleSeconds, MaxConcurrentStreams: runtime.frontend.MaxConcurrentStreams, MaxUseCount: runtime.frontend.MaxUseCount})
	if err != nil {
		return
	}
	defer remote.Close()
	proxyFrontendBytes(local, remote, runtime.frontend.MaxFrameBytes, runtime.frontend.MaxTotalBytes, time.Duration(runtime.frontend.IdleSeconds)*time.Second)
}

func (runtime *applianceFrontendRuntime) close() {
	runtime.closeOnce.Do(func() {
		runtime.cancel()
		runtime.mu.Lock()
		runtime.available = false
		connections := make([]net.Conn, 0, len(runtime.connections))
		for connection := range runtime.connections {
			connections = append(connections, connection)
		}
		runtime.mu.Unlock()
		runtime.listener.Close()
		for _, connection := range connections {
			connection.Close()
		}
		_ = os.Remove(runtime.endpointFile)
	})
}

func proxyFrontendBytes(left, right net.Conn, maxFrame uint32, maxTotal uint64, idle time.Duration) {
	touch := func() {
		deadline := time.Now().Add(idle)
		_ = left.SetDeadline(deadline)
		_ = right.SetDeadline(deadline)
	}
	touch()
	done := make(chan struct{}, 2)
	copyDirection := func(destination, source net.Conn) {
		defer func() { done <- struct{}{} }()
		buffer := make([]byte, maxFrame)
		var total uint64
		for {
			remaining := maxTotal - total
			readBytes := min(uint64(len(buffer)), remaining)
			if readBytes == 0 {
				readBytes = 1
			}
			count, err := source.Read(buffer[:readBytes])
			if count != 0 {
				if uint64(count) > remaining {
					return
				}
				touch()
				if _, writeErr := destination.Write(buffer[:count]); writeErr != nil {
					return
				}
				total += uint64(count)
			}
			if err != nil {
				return
			}
		}
	}
	go copyDirection(right, left)
	go copyDirection(left, right)
	<-done
	left.Close()
	right.Close()
	<-done
}

func writeApplianceFrontendEndpoint(path, endpoint string) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".endpoint-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := io.WriteString(file, endpoint+"\n"); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func writeApplianceFrontendPort(path, endpoint string) error {
	if _, _, err := net.SplitHostPort(endpoint); err != nil {
		return err
	}
	return writeApplianceFrontendEndpoint(path, endpoint)
}
