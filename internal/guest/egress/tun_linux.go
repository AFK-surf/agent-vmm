//go:build linux

package egress

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"golang.org/x/sys/unix"
)

func createTUN() (*os.File, error) {
	if err := ensureTUNDevice(); err != nil {
		return nil, err
	}
	file, err := os.OpenFile("/dev/net/tun", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	request, err := unix.NewIfreq(netprotocol.TunName)
	if err != nil {
		file.Close()
		return nil, err
	}
	request.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(int(file.Fd()), unix.TUNSETIFF, request); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func ensureTUNDevice() error {
	if err := exec.Command("/sbin/modprobe", "tun").Run(); err != nil {
		return err
	}
	if err := os.MkdirAll("/dev/net", 0o755); err != nil {
		return err
	}
	err := unix.Mknod("/dev/net/tun", unix.S_IFCHR|0o600, int(unix.Mkdev(10, 200)))
	if err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	return nil
}

func configureTUN() error {
	commands := [][]string{
		{"link", "set", "dev", netprotocol.TunName, "mtu", "1500", "up"},
		{"addr", "replace", netprotocol.GuestAddress + "/30", "dev", netprotocol.TunName},
		{"route", "replace", "default", "via", netprotocol.HostAddress, "dev", netprotocol.TunName},
	}
	for _, arguments := range commands {
		command := exec.Command("/sbin/ip", arguments...)
		if output, err := command.CombinedOutput(); err != nil {
			return errors.New(string(output))
		}
	}
	return os.WriteFile("/run/resolv.conf", []byte("nameserver "+netprotocol.HostAddress+"\n"), 0o644)
}

func dialHost(ctx context.Context) (net.Conn, error) {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: netprotocol.EgressPort}); err != nil && !errors.Is(err, unix.EINPROGRESS) {
		unix.Close(fd)
		return nil, err
	}
	for {
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
		if _, err := unix.Poll(poll, 100); err != nil && !errors.Is(err, unix.EINTR) {
			unix.Close(fd)
			return nil, err
		}
		if poll[0].Revents&unix.POLLOUT != 0 {
			status, socketErr := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ERROR)
			if socketErr != nil || status != 0 {
				unix.Close(fd)
				if socketErr != nil {
					return nil, socketErr
				}
				return nil, unix.Errno(status)
			}
			if err := unix.SetNonblock(fd, false); err != nil {
				unix.Close(fd)
				return nil, err
			}
			return &vsockConnection{file: os.NewFile(uintptr(fd), "egress-vsock")}, nil
		}
		select {
		case <-ctx.Done():
			unix.Close(fd)
			return nil, ctx.Err()
		default:
		}
	}
}

type vsockConnection struct{ file *os.File }

func (connection *vsockConnection) Read(buffer []byte) (int, error) {
	return connection.file.Read(buffer)
}
func (connection *vsockConnection) Write(buffer []byte) (int, error) {
	return connection.file.Write(buffer)
}
func (connection *vsockConnection) Close() error         { return connection.file.Close() }
func (connection *vsockConnection) LocalAddr() net.Addr  { return vsockAddress("guest") }
func (connection *vsockConnection) RemoteAddr() net.Addr { return vsockAddress("host") }
func (connection *vsockConnection) SetDeadline(value time.Time) error {
	return connection.file.SetDeadline(value)
}
func (connection *vsockConnection) SetReadDeadline(value time.Time) error {
	return connection.file.SetReadDeadline(value)
}
func (connection *vsockConnection) SetWriteDeadline(value time.Time) error {
	return connection.file.SetWriteDeadline(value)
}

type vsockAddress string

func (address vsockAddress) Network() string { return "vsock" }
func (address vsockAddress) String() string  { return string(address) }
