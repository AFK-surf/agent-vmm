//go:build linux

package containerddaemon

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDaemonHelper(t *testing.T) {
	socket := os.Getenv("VMM_TEST_DAEMON_SOCKET")
	if socket == "" {
		return
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		os.Exit(2)
	}
	defer listener.Close()
	if os.Getenv("VMM_TEST_DAEMON_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
	}
	for {
		connection, err := listener.Accept()
		if err != nil {
			os.Exit(3)
		}
		connection.Close()
	}
}

func startDaemon(t *testing.T, socket string, ignore bool) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestDaemonHelper$")
	command.Env = append(os.Environ(), "VMM_TEST_DAEMON_SOCKET="+socket, "VMM_TEST_DAEMON_IGNORE_TERM="+strconv.FormatBool(ignore))
	if ignore {
		command.Env = append(command.Env, "VMM_TEST_DAEMON_IGNORE_TERM=1")
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	deadline := time.Now().Add(5 * time.Second)
	for !IsOwner(socket, command.Process.Pid) {
		if time.Now().After(deadline) {
			t.Fatal("daemon did not own ready socket")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return command
}

func TestStopExactSocketWriterPreservesIndependentProcess(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		t.Run(strconv.FormatBool(ignore), func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "daemon.sock")
			daemon := startDaemon(t, socket, ignore)
			owner, fd, err := peerOwner(socket)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			if owner != daemon.Process.Pid {
				t.Fatal("socket has wrong process owner")
			}
			before, err := Identity(socket)
			if err != nil {
				t.Fatal(err)
			}
			shim := exec.Command("sleep", "30")
			if err := shim.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = shim.Process.Kill(); _ = shim.Wait() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := Stop(ctx, socket); err != nil {
				t.Fatal(err)
			}
			exited, err := ownerExited(fd, 0)
			if err != nil || !exited {
				t.Fatalf("old writer still alive: %v", err)
			}
			_ = daemon.Wait()
			if err := shim.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("independent shim stopped: %v", err)
			}
			// The old pidfd stays bound to the dead lifetime after a replacement starts.
			if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			replacement := startDaemon(t, socket, false)
			after, err := Identity(socket)
			if err != nil || before == after {
				t.Fatalf("replacement lifetime did not change: %q %q %v", before, after, err)
			}
			if err := unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); !errors.Is(err, unix.ESRCH) {
				t.Fatalf("dead pidfd signal = %v", err)
			}
			if !IsOwner(socket, replacement.Process.Pid) {
				t.Fatal("old pidfd affected replacement")
			}
		})
	}
}

func TestStaleSocketIsNotReady(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "stale.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	listener.Close()
	if IsOwner(socket, os.Getpid()) {
		t.Fatal("stale socket counted as ready")
	}
	if err := Stop(context.Background(), socket); err != nil {
		t.Fatal(err)
	}
}

func TestStopDeadlineDoesNotClaimWriterExit(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	daemon := startDaemon(t, socket, true)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if err := Stop(ctx, socket); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop = %v", err)
	}
	if !IsOwner(socket, daemon.Process.Pid) {
		t.Fatal("deadline masqueraded as stopped owner")
	}
}
