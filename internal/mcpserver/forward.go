package mcpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const forwardIdleTimeout = 5 * time.Minute

type forwardLease struct {
	data        ForwardData
	listener    net.Listener
	context     context.Context
	cancel      context.CancelFunc
	adapter     *Adapter
	connections chan struct{}
	mu          sync.Mutex
	active      map[net.Conn]struct{}
	stopOnce    sync.Once
	done        chan struct{}
}

func (adapter *Adapter) registerResourceTools(server *mcp.Server) {
	addTool(server, toolDefinition{name: "port_forward_start", description: "Bind a temporary 127.0.0.1 listener that forwards each connection to one container TCP port.", inputSchema: portForwardStartSchema, timeout: 30 * time.Second}, adapter.portForwardStart)
	addTool(server, toolDefinition{name: "port_forward_stop", description: "Stop one loopback port forward owned by this MCP process.", inputSchema: portForwardStopSchema, destructive: true, timeout: 30 * time.Second}, adapter.portForwardStop)
	addTool(server, toolDefinition{name: "cleanup_execute", description: "Execute a current reset or discard plan after explicit confirmation; the MCP process drains after success.", inputSchema: cleanupExecuteSchema, destructive: true, timeout: 2 * time.Minute}, adapter.cleanupExecute)
}

func (adapter *Adapter) portForwardStart(ctx context.Context, _ *mcp.CallToolRequest, input PortForwardStartInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(ctx context.Context) (any, error) {
		ttl := input.TTLSeconds
		if ttl == 0 {
			ttl = 900
		}
		if input.Port == 0 || ttl < 1 || ttl > 3600 {
			return nil, status.Error(codes.InvalidArgument, "invalid forward bounds")
		}
		response, err := adapter.runtime.GetContainer(ctx, &hostv1.ContainerRequest{ContainerId: input.ContainerID})
		if err != nil {
			return nil, err
		}
		container := response.GetContainer()
		if container.GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING || container.GetInstanceId() == "" {
			return nil, status.Error(codes.FailedPrecondition, "container is not running")
		}
		select {
		case adapter.forwardSlots <- struct{}{}:
		default:
			return nil, status.Error(codes.ResourceExhausted, "forward limit reached")
		}
		address := net.JoinHostPort("127.0.0.1", "0")
		if input.LocalPort != 0 {
			address = net.JoinHostPort("127.0.0.1", strconv.Itoa(int(input.LocalPort)))
		}
		listener, err := net.Listen("tcp4", address)
		if err != nil {
			<-adapter.forwardSlots
			return nil, status.Error(codes.FailedPrecondition, "loopback port unavailable")
		}
		id, err := newRequestID()
		if err != nil {
			listener.Close()
			<-adapter.forwardSlots
			return nil, err
		}
		leaseContext, cancel := context.WithCancel(adapter.lifetime)
		lease := &forwardLease{data: ForwardData{ID: id, ContainerID: input.ContainerID, ContainerInstanceID: container.GetInstanceId(), RemotePort: input.Port, LocalPort: uint32(listener.Addr().(*net.TCPAddr).Port), ExpiresAt: time.Now().Add(time.Duration(ttl) * time.Second)}, listener: listener, context: leaseContext, cancel: cancel, adapter: adapter, connections: make(chan struct{}, 16), active: make(map[net.Conn]struct{}), done: make(chan struct{})}
		adapter.mu.Lock()
		adapter.forwards[id] = lease
		adapter.mu.Unlock()
		go lease.accept()
		go lease.watchContainer()
		time.AfterFunc(time.Duration(ttl)*time.Second, func() { adapter.stopForward(id) })
		return lease.data, nil
	})
}

func (lease *forwardLease) watchContainer() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-lease.context.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(lease.context, 5*time.Second)
			response, err := lease.adapter.runtime.GetContainer(ctx, &hostv1.ContainerRequest{ContainerId: lease.data.ContainerID})
			cancel()
			if err != nil || response.GetContainer().GetState() != hostv1.ContainerState_CONTAINER_STATE_RUNNING || response.GetContainer().GetInstanceId() != lease.data.ContainerInstanceID {
				lease.adapter.stopForward(lease.data.ID)
				return
			}
		}
	}
}

func (adapter *Adapter) portForwardStop(ctx context.Context, _ *mcp.CallToolRequest, input PortForwardStopInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(context.Context) (any, error) {
		if !adapter.stopForward(input.ForwardID) {
			return nil, status.Error(codes.NotFound, "forward not found")
		}
		return EmptyData{}, nil
	})
}

func (adapter *Adapter) stopForward(id string) bool {
	adapter.mu.Lock()
	lease := adapter.forwards[id]
	if lease != nil {
		delete(adapter.forwards, id)
	}
	adapter.mu.Unlock()
	if lease == nil {
		return false
	}
	lease.stop()
	return true
}

func (adapter *Adapter) closeAllForwards() {
	adapter.mu.Lock()
	leases := make([]*forwardLease, 0, len(adapter.forwards))
	for _, lease := range adapter.forwards {
		leases = append(leases, lease)
	}
	clear(adapter.forwards)
	adapter.mu.Unlock()
	for _, lease := range leases {
		lease.stop()
	}
}

func (lease *forwardLease) stop() {
	lease.stopOnce.Do(func() {
		lease.cancel()
		lease.listener.Close()
		lease.mu.Lock()
		for connection := range lease.active {
			connection.Close()
		}
		lease.mu.Unlock()
		<-lease.adapter.forwardSlots
	})
	select {
	case <-lease.done:
	case <-time.After(3 * time.Second):
	}
}

func (lease *forwardLease) accept() {
	var workers sync.WaitGroup
	defer func() { workers.Wait(); close(lease.done) }()
	for {
		connection, err := lease.listener.Accept()
		if err != nil {
			return
		}
		select {
		case lease.connections <- struct{}{}:
		default:
			connection.Close()
			continue
		}
		select {
		case lease.adapter.connectionSlots <- struct{}{}:
		default:
			<-lease.connections
			connection.Close()
			continue
		}
		lease.mu.Lock()
		lease.active[connection] = struct{}{}
		lease.mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				<-lease.connections
				<-lease.adapter.connectionSlots
				lease.mu.Lock()
				delete(lease.active, connection)
				lease.mu.Unlock()
				connection.Close()
			}()
			lease.bridge(connection)
		}()
	}
}

func (lease *forwardLease) bridge(connection net.Conn) {
	stream, err := lease.adapter.runtime.ForwardTCP(lease.context)
	if err != nil {
		return
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Header{Header: &hostv1.ForwardTCPHeader{ContainerId: lease.data.ContainerID, ContainerInstanceId: lease.data.ContainerInstanceID, Port: lease.data.RemotePort}}}); err != nil {
		return
	}
	done := make(chan error, 2)
	go func() { done <- localToRuntime(connection, stream) }()
	go func() { done <- runtimeToLocal(connection, stream) }()
	first := <-done
	if first != nil {
		connection.Close()
		stream.CloseSend()
	}
	second := <-done
	_ = stream.CloseSend()
	if lease.context.Err() == nil && (runtimeFailed(first) || runtimeFailed(second)) {
		go lease.adapter.stopForward(lease.data.ID)
	}
}

func localToRuntime(connection net.Conn, stream hostv1.AgentRuntimeService_ForwardTCPClient) error {
	buffer := make([]byte, 32<<10)
	for {
		_ = connection.SetReadDeadline(time.Now().Add(forwardIdleTimeout))
		count, err := connection.Read(buffer)
		if count > 0 {
			if sendErr := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Data{Data: append([]byte(nil), buffer[:count]...)}}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			_ = stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}})
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func runtimeToLocal(connection net.Conn, stream hostv1.AgentRuntimeService_ForwardTCPClient) error {
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if data := response.GetData(); len(data) > 0 {
			_ = connection.SetWriteDeadline(time.Now().Add(forwardIdleTimeout))
			if _, err := connection.Write(data); err != nil {
				return err
			}
		}
		if response.GetCloseRead() {
			if tcp, ok := connection.(*net.TCPConn); ok {
				return tcp.CloseWrite()
			}
			return nil
		}
	}
}

func runtimeFailed(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.PermissionDenied, codes.Unauthenticated, codes.Unavailable:
		return true
	default:
		return false
	}
}

const portForwardStartSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id","port"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"port":{"type":"integer","minimum":1,"maximum":65535},"local_port":{"type":"integer","minimum":0,"maximum":65535},"ttl_seconds":{"type":"integer","minimum":1,"maximum":3600}}}`
const portForwardStopSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["forward_id"],"properties":{"forward_id":{"type":"string","minLength":32,"maxLength":128}}}`
const cleanupExecuteSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["kind","plan_revision"],"properties":{"kind":{"enum":["reset","discard"]},"plan_revision":{"type":"string","pattern":"^[1-9][0-9]{0,19}$"},"confirm_reset":{"type":"boolean"},"confirm_irreversible":{"type":"boolean"},"confirmed_volume_ids":{"type":"array","maxItems":1000,"uniqueItems":true,"items":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"}}}}`
