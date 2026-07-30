//go:build linux

package vsock

import (
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type Listener struct {
	file *os.File
}

func Listen(port uint32) (*Listener, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
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
	fd, _, err := unix.Accept4(int(listener.file.Fd()), unix.SOCK_CLOEXEC)
	if err != nil {
		return nil, err
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
