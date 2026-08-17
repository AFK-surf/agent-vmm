//go:build darwin

package host

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
)

type systemStreamConn struct {
	stream   grpc.BidiStreamingClient[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame]
	frameMax uint32

	readMu  sync.Mutex
	readBuf []byte
	writeMu sync.Mutex
	closed  sync.Once
}

func openSystemStreamConn(ctx context.Context, client vmmdv1.VMMDServiceClient, header *vmmdv1.SystemStreamHeader) (net.Conn, error) {
	stream, err := client.OpenSystemStream(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Header{Header: header}}); err != nil {
		return nil, err
	}
	return &systemStreamConn{stream: stream, frameMax: header.GetMaxFrameBytes()}, nil
}

func (connection *systemStreamConn) Read(output []byte) (int, error) {
	connection.readMu.Lock()
	defer connection.readMu.Unlock()
	if len(connection.readBuf) != 0 {
		count := copy(output, connection.readBuf)
		connection.readBuf = connection.readBuf[count:]
		return count, nil
	}
	for {
		frame, err := connection.stream.Recv()
		if err != nil {
			return 0, err
		}
		if frame.GetCloseWrite() {
			return 0, io.EOF
		}
		data := frame.GetData()
		if len(data) == 0 || uint32(len(data)) > connection.frameMax {
			return 0, errors.New("guest returned an invalid system stream frame")
		}
		count := copy(output, data)
		connection.readBuf = append(connection.readBuf[:0], data[count:]...)
		return count, nil
	}
}

func (connection *systemStreamConn) Write(input []byte) (int, error) {
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	written := 0
	for len(input) != 0 {
		count := min(len(input), int(connection.frameMax))
		if err := connection.stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: append([]byte(nil), input[:count]...)}}); err != nil {
			return written, err
		}
		written += count
		input = input[count:]
	}
	return written, nil
}

func (connection *systemStreamConn) Close() error {
	var err error
	connection.closed.Do(func() {
		err = connection.stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_CloseWrite{CloseWrite: true}})
		if closeErr := connection.stream.CloseSend(); err == nil {
			err = closeErr
		}
	})
	return err
}

func (*systemStreamConn) LocalAddr() net.Addr              { return systemStreamAddress("host") }
func (*systemStreamConn) RemoteAddr() net.Addr             { return systemStreamAddress("appliance") }
func (*systemStreamConn) SetDeadline(time.Time) error      { return nil }
func (*systemStreamConn) SetReadDeadline(time.Time) error  { return nil }
func (*systemStreamConn) SetWriteDeadline(time.Time) error { return nil }

type systemStreamAddress string

func (address systemStreamAddress) Network() string { return "system-stream" }
func (address systemStreamAddress) String() string  { return string(address) }
