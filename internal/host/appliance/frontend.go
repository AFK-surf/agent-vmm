//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

var applianceFrontendID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

type applianceFrontendRuntime struct {
	controller        *Owner
	installationID    string
	name              string
	containerInstance string
	endpoint          string
	frontend          store.ApplianceFrontend
	identity          *vmmdv1.SystemApplianceIdentity
	listener          net.Listener
	open              func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error)
	ctx               context.Context
	cancel            context.CancelFunc

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	errorReason string
	available   bool
	closeOnce   sync.Once
}

func applianceFrontendPortPath(supportDirectory, installationID, frontendName string) string {
	return filepath.Join(supportDirectory, "appliance-frontend-ports", installationID, frontendName+".port")
}

func (controller *Owner) startApplianceFrontends(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity) error {
	if identity == nil || identity.GetInstallationId() != installation.ID || identity.GetContainerInstanceId() == "" {
		return errors.New("frontend identity does not match the appliance")
	}
	frontends, err := controller.store.ListApplianceFrontends(ctx, installation.ID)
	if err != nil {
		return err
	}
	wanted := make(map[string]store.ApplianceFrontend, len(frontends))
	for _, frontend := range frontends {
		if frontend.Exposure != int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY) {
			wanted[frontendRuntimeKey(installation.ID, frontend.Name)] = frontend
		}
	}
	controller.applianceFrontendMu.Lock()
	var stale []*applianceFrontendRuntime
	for key, runtime := range controller.applianceFrontends {
		if runtime.installationID != installation.ID {
			continue
		}
		frontend, exists := wanted[key]
		runtime.mu.Lock()
		available := runtime.available
		runtime.mu.Unlock()
		if exists && runtime.frontend == frontend && runtime.containerInstance == identity.GetContainerInstanceId() && runtime.identity.GetBootId() == identity.GetBootId() && available {
			delete(wanted, key)
			continue
		}
		delete(controller.applianceFrontends, key)
		stale = append(stale, runtime)
	}
	controller.applianceFrontendMu.Unlock()
	for _, runtime := range stale {
		runtime.close()
	}
	for _, frontend := range frontends {
		if frontend.Exposure == int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY) {
			continue
		}
		key := frontendRuntimeKey(installation.ID, frontend.Name)
		if _, missing := wanted[key]; !missing {
			continue
		}
		runtime, err := controller.newApplianceFrontendRuntime(frontend, identity, func(streamCtx context.Context, header *vmmdv1.SystemStreamHeader) (net.Conn, error) {
			client, err := controller.guestConnection.Client()
			if err != nil {
				return nil, err
			}
			return openSystemStreamConn(streamCtx, client, header)
		})
		if err != nil {
			controller.logger.Warn("appliance frontend unavailable", "installation_id", installation.ID, "frontend", frontend.Name, "error", err)
			runtimeCtx, cancel := context.WithCancel(context.Background())
			runtime = &applianceFrontendRuntime{installationID: installation.ID, name: frontend.Name, frontend: frontend,
				containerInstance: identity.GetContainerInstanceId(), identity: proto.Clone(identity).(*vmmdv1.SystemApplianceIdentity),
				ctx: runtimeCtx, cancel: cancel, errorReason: "FRONTEND_UNAVAILABLE"}
			var unavailable *frontendPortUnavailable
			if errors.As(err, &unavailable) {
				runtime.endpoint = unavailable.endpoint
				runtime.errorReason = "FRONTEND_PORT_UNAVAILABLE"
			}
		}
		controller.applianceFrontendMu.Lock()
		controller.applianceFrontends[key] = runtime
		controller.applianceFrontendMu.Unlock()
		if err == nil {
			go runtime.serve()
		}
	}
	return nil
}

type frontendPortUnavailable struct {
	endpoint string
	err      error
}

func (err *frontendPortUnavailable) Error() string {
	return fmt.Sprintf("FRONTEND_PORT_UNAVAILABLE: %s: %v", err.endpoint, err.err)
}
func (err *frontendPortUnavailable) Unwrap() error { return err.err }

func (controller *Owner) newApplianceFrontendRuntime(frontend store.ApplianceFrontend, identity *vmmdv1.SystemApplianceIdentity, open func(context.Context, *vmmdv1.SystemStreamHeader) (net.Conn, error)) (*applianceFrontendRuntime, error) {
	if (frontend.Exposure != int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK) && frontend.Exposure != int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP)) ||
		(frontend.Exposure == int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP) && frontend.Transport != int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP)) ||
		(frontend.Transport != int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_BYTE_STREAM) && frontend.Transport != int32(appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP)) {
		return nil, fmt.Errorf("frontend %q has an unsupported projection", frontend.Name)
	}
	if !applianceFrontendID.MatchString(identity.GetInstallationId()) || !applianceFrontendID.MatchString(frontend.Name) {
		return nil, errors.New("frontend projection identity is invalid")
	}
	if frontend.MaxFrameBytes == 0 || frontend.MaxFrameBytes > 1<<20 || (frontend.MaxTotalBytes != 0 && frontend.MaxTotalBytes < uint64(frontend.MaxFrameBytes)) ||
		frontend.DeadlineSeconds == 0 || frontend.DeadlineSeconds > 24*60*60 || frontend.IdleSeconds == 0 || frontend.IdleSeconds > 300 ||
		frontend.MaxConcurrentStreams == 0 || frontend.MaxConcurrentStreams > 16 {
		return nil, fmt.Errorf("frontend %q has incomplete stream bounds", frontend.Name)
	}
	portPath := applianceFrontendPortPath(controller.supportDirectory, identity.GetInstallationId(), frontend.Name)
	bindHost := "127.0.0.1"
	if frontend.Exposure == int32(appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP) {
		var err error
		bindHost, err = applianceLANBindHost()
		if err != nil {
			return nil, err
		}
	}
	listenAddress := net.JoinHostPort(bindHost, "0")
	persisted, err := os.ReadFile(portPath)
	if err == nil {
		listenAddress = strings.TrimSpace(string(persisted))
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("tcp4", listenAddress)
	if err != nil {
		return nil, &frontendPortUnavailable{endpoint: listenAddress, err: err}
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	runtime := &applianceFrontendRuntime{controller: controller, installationID: identity.GetInstallationId(), name: frontend.Name,
		containerInstance: identity.GetContainerInstanceId(), endpoint: listener.Addr().String(),
		frontend: frontend, identity: proto.Clone(identity).(*vmmdv1.SystemApplianceIdentity), listener: listener,
		open: open, ctx: runtimeCtx, cancel: cancel, connections: make(map[net.Conn]struct{}), available: true}
	if persisted == nil {
		if err := writeApplianceFrontendPort(portPath, runtime.endpoint); err != nil {
			cancel()
			listener.Close()
			return nil, err
		}
	}
	return runtime, nil
}

func frontendRuntimeKey(installationID, name string) string { return installationID + "/" + name }

func (controller *Owner) stopApplianceFrontends(installationID string) {
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

func (controller *Owner) StopAllFrontends() {
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

func (controller *Owner) applianceFrontendProjection(installationID, name, containerInstance string) (string, bool, string) {
	controller.applianceFrontendMu.Lock()
	runtime := controller.applianceFrontends[frontendRuntimeKey(installationID, name)]
	controller.applianceFrontendMu.Unlock()
	if runtime == nil || runtime.containerInstance != containerInstance {
		return "", false, ""
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.endpoint, runtime.available, runtime.errorReason
}

func (runtime *applianceFrontendRuntime) serve() {
	for {
		connection, err := runtime.listener.Accept()
		if err != nil {
			return
		}
		if !runtime.reserve(connection) {
			connection.Close()
			continue
		}
		go runtime.proxy(runtime.ctx, connection)
	}
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
		if runtime.listener != nil {
			runtime.listener.Close()
		}
		for _, connection := range connections {
			connection.Close()
		}
	})
}

func writeApplianceFrontendPort(path, endpoint string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return atomicfile.ReplaceFile(path, []byte(endpoint+"\n"), 0o600)
}

func applianceLANBindHost() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var candidates []netip.Addr
	for _, device := range interfaces {
		if device.Flags&net.FlagUp == 0 || device.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := device.Addrs()
		if err != nil {
			return "", err
		}
		for _, value := range addresses {
			prefix, err := netip.ParsePrefix(value.String())
			if err == nil && prefix.Addr().Is4() && prefix.Addr().IsPrivate() {
				candidates = append(candidates, prefix.Addr())
			}
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("host has no private IPv4 interface for LAN frontend")
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Less(candidates[j]) })
	return candidates[0].String(), nil
}
