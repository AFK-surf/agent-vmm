//go:build darwin || linux

package main

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

// host-socket set|remove|list: the operator approves one Host Unix socket at one port of the Host
// virtual address for an environment's containers (docs/design/networking.md, Host sockets).
func (runtime managedRuntime) hostSocket(ctx context.Context, action string, opts managedOptions) (managedEnvelope, error) {
	envelope := baseManagedEnvelope("host-socket", runtime.now().UTC(), map[string]any{"environment_id": opts.environmentID})
	envelope.Source = []string{"host_operator"}
	envelope.Freshness = "current"
	connection, err := managedOperatorConnection(opts.operatorSocket)
	if err != nil {
		return managedEnvelope{}, err
	}
	defer connection.Close()
	client := runtime.hostSockets
	if client == nil {
		client = hostv1.NewOperatorServiceClient(connection)
	}
	switch action {
	case "set":
		response, err := client.SetHostSocket(ctx, &hostv1.SetHostSocketRequest{RequestId: opts.requestID, EnvironmentId: opts.environmentID, Port: uint32(opts.port), Path: opts.path})
		if err != nil {
			return managedEnvelope{}, err
		}
		envelope.State, envelope.Stage = "approved", "set"
		envelope.Facts = map[string]any{"host_sockets": []map[string]any{managedHostSocket(response.GetHostSocket())}}
	case "remove":
		if _, err := client.RemoveHostSocket(ctx, &hostv1.RemoveHostSocketRequest{RequestId: opts.requestID, EnvironmentId: opts.environmentID, Port: uint32(opts.port)}); err != nil {
			return managedEnvelope{}, err
		}
		envelope.State, envelope.Stage = "removed", "remove"
		envelope.Facts = map[string]any{"port": opts.port}
	default:
		request := &hostv1.ListHostSocketsRequest{}
		if opts.environmentID != "" {
			request.EnvironmentId = &opts.environmentID
		}
		response, err := client.ListHostSockets(ctx, request)
		if err != nil {
			return managedEnvelope{}, err
		}
		sockets := []map[string]any{}
		for _, socket := range response.GetHostSockets() {
			sockets = append(sockets, managedHostSocket(socket))
		}
		envelope.State, envelope.Stage = "observed", "list"
		envelope.Facts = map[string]any{"host_sockets": sockets}
	}
	return envelope, nil
}

func managedHostSocket(socket *hostv1.HostSocket) map[string]any {
	return map[string]any{"environment_id": socket.GetEnvironmentId(), "port": socket.GetPort(), "path": socket.GetPath(), "revision": socket.GetRevision()}
}
