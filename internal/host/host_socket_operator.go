//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Host sockets (docs/design/networking.md): the operator approves one Host Unix socket at one port
// of the Host virtual address for an environment, controller-owned or not, or for every environment
// of this Host, as the machine's owner approves what of the machine its environments reach. The Host
// egress connects each such flow to the socket; nothing else of the Host loopback or LAN is reached.

func (server *operatorServer) ListHostSockets(ctx context.Context, request *hostv1.ListHostSocketsRequest) (*hostv1.ListHostSocketsResponse, error) {
	values, err := server.controller.store.ListHostSockets(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, err
	}
	response := new(hostv1.ListHostSocketsResponse)
	for _, value := range values {
		response.HostSockets = append(response.HostSockets, hostSocketToProto(value))
	}
	return response, nil
}

func (server *operatorServer) SetHostSocket(ctx context.Context, request *hostv1.SetHostSocketRequest) (*hostv1.SetHostSocketResponse, error) {
	port, err := hostSocketTarget(request.GetRequestId(), request.GetPort())
	if err != nil {
		return nil, err
	}
	path := request.GetPath()
	// (the socket may come later: each flow connects to the path as it is then)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, hostv1.InvalidField("path", "an absolute, clean Host path is required")
	}
	var value store.HostSocket
	err = server.changeHostSockets(ctx, request.GetEnvironmentId(), func(ctx context.Context) (err error) {
		value, err = server.controller.store.SetHostSocket(ctx, request.GetEnvironmentId(), port, path)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &hostv1.SetHostSocketResponse{HostSocket: hostSocketToProto(value)}, nil
}

func (server *operatorServer) RemoveHostSocket(ctx context.Context, request *hostv1.RemoveHostSocketRequest) (*hostv1.RemoveHostSocketResponse, error) {
	port, err := hostSocketTarget(request.GetRequestId(), request.GetPort())
	if err != nil {
		return nil, err
	}
	err = server.changeHostSockets(ctx, request.GetEnvironmentId(), func(ctx context.Context) error {
		if err := server.controller.store.RemoveHostSocket(ctx, request.GetEnvironmentId(), port); errors.Is(err, sql.ErrNoRows) {
			return status.Error(codes.NotFound, "the environment has no Host socket at this port")
		} else {
			return err
		}
	})
	if err != nil {
		return nil, err
	}
	return &hostv1.RemoveHostSocketResponse{}, nil
}

func hostSocketTarget(requestID string, port uint32) (uint16, error) {
	if requestID == "" {
		return 0, hostv1.InvalidField("request_id", "request ID is required")
	}
	// (53 is the Host virtual address's DNS)
	if port == 0 || port > 65535 || port == 53 {
		return 0, hostv1.InvalidField("port", "a TCP port other than 53 is required")
	}
	return uint16(port), nil
}

// changeHostSockets changes the sockets of an environment (under its lock) or of every environment (no
// environment ID) under the policy projection lock, then publishes the egress policy: a flow to a
// socket removed or moved ends with it.
func (server *operatorServer) changeHostSockets(ctx context.Context, environmentID string, change func(context.Context) error) error {
	ctx, releaseInventory, err := server.controller.inventory.Enter(ctx)
	if err != nil {
		return err
	}
	defer releaseInventory()
	if environmentID != "" {
		unlock, err := server.controller.workloads.LockEnvironment(ctx, environmentID)
		if err != nil {
			return err
		}
		defer unlock()
		if _, err := server.controller.store.GetEnvironment(ctx, environmentID); errors.Is(err, sql.ErrNoRows) {
			return status.Error(codes.NotFound, "agent environment does not exist")
		} else if err != nil {
			return err
		}
	}
	if err := server.controller.services.ProjectionLock().Lock(ctx); err != nil {
		return err
	}
	defer server.controller.services.ProjectionLock().Unlock()
	if err := change(ctx); err != nil {
		return err
	}
	if server.controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return nil
	}
	if err := server.controller.services.PublishSourcePoliciesLocked(ctx, ""); err != nil {
		return hostv1.NewError(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_POLICY_PROJECTION_FAILED, "the Host socket was saved but the egress policy was not published", hostv1.WithRetry(time.Second))
	}
	return nil
}

func hostSocketToProto(value store.HostSocket) *hostv1.HostSocket {
	return &hostv1.HostSocket{EnvironmentId: value.EnvironmentID, Port: uint32(value.Port), Path: value.Path, Revision: value.Revision}
}
