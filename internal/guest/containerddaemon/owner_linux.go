//go:build linux

package containerddaemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Identity binds the socket's actual writer process to its kernel lifetime.
func Identity(socket string) (string, error) {
	pid, fd, err := peerOwner(socket)
	if err != nil {
		return "", err
	}
	defer unix.Close(fd)
	identity, err := processIdentity(pid)
	if err != nil {
		return "", err
	}
	exited, err := ownerExited(fd, 0)
	if err != nil {
		return "", err
	}
	if exited {
		return "", unix.ESRCH
	}
	return identity, nil
}

func peerOwner(socket string) (int, int, error) {
	connection, err := net.DialTimeout("unix", socket, time.Second)
	if err != nil {
		return 0, -1, err
	}
	defer connection.Close()
	raw, err := connection.(*net.UnixConn).SyscallConn()
	if err != nil {
		return 0, -1, err
	}
	var credentials *unix.Ucred
	var credentialErr error
	fd := -1
	if err := raw.Control(func(socketFD uintptr) {
		credentials, credentialErr = unix.GetsockoptUcred(int(socketFD), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if credentialErr == nil {
			fd, credentialErr = unix.GetsockoptInt(int(socketFD), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		}
	}); err != nil {
		return 0, -1, err
	}
	if credentialErr != nil {
		return 0, -1, credentialErr
	}
	return int(credentials.Pid), fd, nil
}

func ownerExited(fd, timeout int) (bool, error) {
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	ready, err := unix.Poll(poll, timeout)
	return ready > 0, err
}

func processIdentity(pid int) (string, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	// comm can contain spaces and parentheses. Fields after its last ')' start at field 3.
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) <= 19 {
		return "", errors.New("process stat lacks start time")
	}
	return strings.TrimSpace(string(boot)) + ":" + strconv.Itoa(pid) + ":" + fields[19], nil
}

// Stop waits for the exact previous socket owner. It leaves independent shims alone.
func Stop(ctx context.Context, socket string) error {
	_, fd, err := peerOwner(socket)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ECONNREFUSED) {
			return nil
		}
		return err
	}
	defer unix.Close(fd)
	if err := unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
		return err
	}
	killAt := time.Now().Add(2 * time.Second)
	killed := false
	for {
		exited, err := ownerExited(fd, 50)
		if err != nil && !errors.Is(err, unix.EINTR) {
			return err
		}
		if exited {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !killed && time.Now().After(killAt) {
			if err := unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
				return err
			}
			killed = true
		}
	}
}

func IsOwner(socket string, pid int) bool {
	owner, fd, err := peerOwner(socket)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	exited, err := ownerExited(fd, 0)
	return err == nil && !exited && owner == pid
}
