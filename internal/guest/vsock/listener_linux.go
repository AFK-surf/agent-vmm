//go:build linux

package vsock

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type Listener struct {
	file *os.File
}

func Listen(port uint32) (net.Listener, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: port}); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if err := unix.Listen(fd, 128); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return &Listener{file: os.NewFile(uintptr(fd), "vsock-listener")}, nil
}

func (listener *Listener) Accept() (net.Conn, error) {
	raw, err := listener.file.SyscallConn()
	if err != nil {
		return nil, err
	}
	var fd int
	var acceptErr error
	err = raw.Read(func(socket uintptr) bool {
		fd, _, acceptErr = unix.Accept4(int(socket), unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		return !errors.Is(acceptErr, unix.EAGAIN) && !errors.Is(acceptErr, unix.EINTR)
	})
	if err != nil {
		return nil, err
	}
	if acceptErr != nil {
		return nil, acceptErr
	}
	return &connection{file: os.NewFile(uintptr(fd), "vsock-connection")}, nil
}

func (listener *Listener) Close() error   { return listener.file.Close() }
func (listener *Listener) Addr() net.Addr { return address("vsock") }

type connection struct {
	file *os.File
}

func (connection *connection) Read(buffer []byte) (int, error)  { return connection.file.Read(buffer) }
func (connection *connection) Write(buffer []byte) (int, error) { return connection.file.Write(buffer) }
func (connection *connection) Close() error                     { return connection.file.Close() }
func (connection *connection) LocalAddr() net.Addr              { return address("vsock-local") }
func (connection *connection) RemoteAddr() net.Addr             { return address("vsock-remote") }
func (connection *connection) SetDeadline(deadline time.Time) error {
	return connection.file.SetDeadline(deadline)
}
func (connection *connection) SetReadDeadline(deadline time.Time) error {
	return connection.file.SetReadDeadline(deadline)
}
func (connection *connection) SetWriteDeadline(deadline time.Time) error {
	return connection.file.SetWriteDeadline(deadline)
}

type address string

func (address address) Network() string { return "vsock" }
func (address address) String() string  { return fmt.Sprintf("%s", string(address)) }

// Dial connects through the poller so cancellation and deadlines wake I/O.
func Dial(ctx context.Context, cid, port uint32) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	connectErr := unix.Connect(fd, &unix.SockaddrVM{CID: cid, Port: port})
	if connectErr != nil && !errors.Is(connectErr, unix.EINPROGRESS) {
		unix.Close(fd)
		return nil, connectErr
	}
	file := os.NewFile(uintptr(fd), "vsock-connection")
	if connectErr == nil {
		return &connection{file: file}, nil
	}
	raw, err := file.SyscallConn()
	if err != nil {
		file.Close()
		return nil, err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = file.SetWriteDeadline(time.Now()); close(canceled) })
	var socketErr error
	err = raw.Write(func(socket uintptr) bool {
		failure, readErr := unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_ERROR)
		if readErr != nil {
			socketErr = readErr
			return true
		}
		if failure != 0 {
			socketErr = unix.Errno(failure)
			return true
		}
		_, peerErr := unix.Getpeername(int(socket))
		if errors.Is(peerErr, unix.ENOTCONN) {
			return false
		}
		socketErr = peerErr
		return true
	})
	if !stop() {
		<-canceled
	}
	if ctx.Err() != nil {
		file.Close()
		return nil, ctx.Err()
	}
	if err != nil || socketErr != nil {
		file.Close()
		return nil, errors.Join(err, socketErr)
	}
	if err := file.SetWriteDeadline(time.Time{}); err != nil {
		file.Close()
		return nil, err
	}
	return &connection{file: file}, nil
}
