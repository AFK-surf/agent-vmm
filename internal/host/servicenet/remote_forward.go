//go:build darwin || linux

package servicenet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxServiceFrameBytes = 64 << 10

func (controller *Owner) dialRemoteService(ctx context.Context, header *servicev1.OpenServiceForwardHeader, observation *trustv1.EndpointObservation) (net.Conn, error) {
	if controller.dataWorkerSocket == "" || observation == nil || len(observation.GetEndpointNodeId()) == 0 {
		return nil, errors.New("remote service data path is unavailable")
	}
	endpoint, err := json.Marshal(map[string]string{"id": string(observation.GetEndpointNodeId())})
	if err != nil {
		return nil, err
	}
	deadline := header.GetCapability().GetExpiresAt().AsTime()
	streamContext, cancel := context.WithDeadline(ctx, deadline)
	connection, err := grpc.NewClient("passthrough:///agent-vmm-service", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dialContext context.Context, _ string) (net.Conn, error) {
			return dataworker.DialServiceTunnel(dialContext, controller.dataWorkerSocket, endpoint)
		}))
	if err != nil {
		cancel()
		return nil, err
	}
	stream, err := servicev1.NewServiceRouteServiceClient(connection).OpenServiceForward(streamContext)
	if err != nil {
		cancel()
		connection.Close()
		return nil, err
	}
	if err := stream.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_Header{Header: header}}); err != nil {
		cancel()
		connection.Close()
		return nil, err
	}
	budget := header.GetCapability().GetBudget().GetByteLimit()
	return &serviceForwardConn{stream: stream, connection: connection, cancel: cancel, budget: budget}, nil
}

type serviceForwardConn struct {
	stream     servicev1.ServiceRouteService_OpenServiceForwardClient
	connection *grpc.ClientConn
	cancel     context.CancelFunc
	readBuffer bytes.Reader
	readMu     sync.Mutex
	writeMu    sync.Mutex
	used       atomic.Uint64
	budget     uint64
	closed     atomic.Bool
}

func (connection *serviceForwardConn) Read(value []byte) (int, error) {
	connection.readMu.Lock()
	defer connection.readMu.Unlock()
	for connection.readBuffer.Len() == 0 {
		response, err := connection.stream.Recv()
		if err != nil {
			return 0, err
		}
		if response.GetCloseWrite() {
			return 0, io.EOF
		}
		data := response.GetData()
		if len(data) == 0 || !connection.consume(uint64(len(data))) {
			return 0, errors.New("service route byte budget exhausted")
		}
		connection.readBuffer.Reset(data)
	}
	return connection.readBuffer.Read(value)
}

func (connection *serviceForwardConn) Write(value []byte) (int, error) {
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	if connection.closed.Load() || !connection.consume(uint64(len(value))) {
		return 0, errors.New("service route is closed or byte budget exhausted")
	}
	written := 0
	for len(value) > 0 {
		count := min(len(value), maxServiceFrameBytes)
		if err := connection.stream.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_Data{Data: append([]byte(nil), value[:count]...)}}); err != nil {
			return written, err
		}
		written += count
		value = value[count:]
	}
	return written, nil
}

func (connection *serviceForwardConn) consume(count uint64) bool {
	for {
		used := connection.used.Load()
		if connection.budget == 0 || used >= connection.budget || count > connection.budget-used {
			return false
		}
		if connection.used.CompareAndSwap(used, used+count) {
			return true
		}
	}
}

func (connection *serviceForwardConn) Close() error {
	if !connection.closed.CompareAndSwap(false, true) {
		return nil
	}
	connection.writeMu.Lock()
	_ = connection.stream.Send(&servicev1.OpenServiceForwardRequest{Payload: &servicev1.OpenServiceForwardRequest_CloseWrite{CloseWrite: true}})
	_ = connection.stream.CloseSend()
	connection.writeMu.Unlock()
	connection.cancel()
	return connection.connection.Close()
}

func (connection *serviceForwardConn) LocalAddr() net.Addr              { return serviceAddr("local") }
func (connection *serviceForwardConn) RemoteAddr() net.Addr             { return serviceAddr("remote") }
func (connection *serviceForwardConn) SetDeadline(time.Time) error      { return nil }
func (connection *serviceForwardConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *serviceForwardConn) SetWriteDeadline(time.Time) error { return nil }

type serviceAddr string

func (address serviceAddr) Network() string { return "agent-vmm-service" }
func (address serviceAddr) String() string  { return string(address) }
