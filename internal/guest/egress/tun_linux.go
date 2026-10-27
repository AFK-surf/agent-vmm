//go:build linux

package egress

import (
	"errors"
	"io"
	"os"
	"os/exec"

	"github.com/AFK-surf/agent-vmm/internal/egresslink"
	"golang.org/x/sys/unix"
)

type tunDevice struct {
	file *os.File
}

func (device *tunDevice) Read(buffer []byte) (int, error) {
	for {
		count, err := unix.Read(int(device.file.Fd()), buffer)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return count, os.NewSyscallError("read", err)
		}
		return count, nil
	}
}

func (device *tunDevice) Write(packet []byte) (int, error) {
	for {
		count, err := unix.Write(int(device.file.Fd()), packet)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return count, os.NewSyscallError("write", err)
		}
		if count != len(packet) {
			return count, io.ErrShortWrite
		}
		return count, nil
	}
}

func (device *tunDevice) Close() error {
	return device.file.Close()
}

// TUN packet I/O uses blocking syscalls because /dev/net/tun may reject the Go poller.
func createTUN() (io.ReadWriteCloser, error) {
	if err := ensureTUNDevice(); err != nil {
		return nil, err
	}
	file, err := os.OpenFile("/dev/net/tun", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	request, err := unix.NewIfreq(egresslink.TunName)
	if err != nil {
		file.Close()
		return nil, err
	}
	request.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(int(file.Fd()), unix.TUNSETIFF, request); err != nil {
		file.Close()
		return nil, err
	}
	if err := unix.SetNonblock(int(file.Fd()), false); err != nil {
		file.Close()
		return nil, err
	}
	return &tunDevice{file: file}, nil
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
		{"link", "set", "dev", egresslink.TunName, "mtu", "1500", "up"},
		{"addr", "replace", egresslink.GuestAddress + "/30", "dev", egresslink.TunName},
		{"route", "replace", "default", "via", egresslink.HostAddress, "dev", egresslink.TunName},
	}
	for _, arguments := range commands {
		command := exec.Command("/sbin/ip", arguments...)
		if output, err := command.CombinedOutput(); err != nil {
			return errors.New(string(output))
		}
	}
	return os.WriteFile("/run/resolv.conf", []byte("nameserver "+egresslink.HostAddress+"\n"), 0o644)
}
