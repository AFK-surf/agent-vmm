package remote

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

type SessionStream interface {
	Send(*remotev1.ControllerSessionFrame) error
	Recv() (*remotev1.HostSessionFrame, error)
	Context() context.Context
}

type SessionConnection interface {
	net.Conn
	Done() <-chan struct{}
}

func AcceptSession(stream SessionStream, registrationID string, connectionEpoch uint64) (*remotev1.SessionHeader, SessionConnection, error) {
	first, err := stream.Recv()
	if err != nil {
		return nil, nil, err
	}
	header := first.GetHeader()
	if header == nil || header.GetRegistrationId() != registrationID || header.GetConnectionEpoch() != connectionEpoch || header.GetAllocationId() == "" || header.GetLeaseId() == "" || header.GetGeneration() == 0 {
		return nil, nil, errors.New("invalid remote session header")
	}
	return header, &sessionConn{stream: stream, closed: make(chan struct{})}, nil
}

type sessionConn struct {
	stream  SessionStream
	readMu  sync.Mutex
	writeMu sync.Mutex
	buffer  []byte
	closed  chan struct{}
	once    sync.Once
}

func (connection *sessionConn) Read(value []byte) (int, error) {
	connection.readMu.Lock()
	defer connection.readMu.Unlock()
	for len(connection.buffer) == 0 {
		frame, err := connection.stream.Recv()
		if err != nil {
			return 0, err
		}
		if frame.GetCloseWrite() {
			return 0, io.EOF
		}
		connection.buffer = frame.GetData()
	}
	count := copy(value, connection.buffer)
	connection.buffer = connection.buffer[count:]
	return count, nil
}

func (connection *sessionConn) Write(value []byte) (int, error) {
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	total := len(value)
	for len(value) != 0 {
		count := min(len(value), 32<<10)
		if err := connection.stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_Data{Data: append([]byte(nil), value[:count]...)}}); err != nil {
			return total - len(value), err
		}
		value = value[count:]
	}
	return total, nil
}

func (connection *sessionConn) Close() error {
	connection.once.Do(func() {
		connection.writeMu.Lock()
		_ = connection.stream.Send(&remotev1.ControllerSessionFrame{Payload: &remotev1.ControllerSessionFrame_CloseWrite{CloseWrite: true}})
		connection.writeMu.Unlock()
		close(connection.closed)
	})
	return nil
}

func (connection *sessionConn) Done() <-chan struct{} { return connection.closed }

func (*sessionConn) LocalAddr() net.Addr              { return sessionAddr("controller") }
func (*sessionConn) RemoteAddr() net.Addr             { return sessionAddr("host") }
func (*sessionConn) SetDeadline(time.Time) error      { return nil }
func (*sessionConn) SetReadDeadline(time.Time) error  { return nil }
func (*sessionConn) SetWriteDeadline(time.Time) error { return nil }

type sessionAddr string

func (sessionAddr) Network() string        { return "agent-vmm-session" }
func (address sessionAddr) String() string { return string(address) }
