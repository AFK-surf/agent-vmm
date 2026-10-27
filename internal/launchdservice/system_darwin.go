package launchdservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"golang.org/x/sys/unix"
)

var ErrNotFound = errors.New("launchd service is not loaded")

type System struct {
	Launchctl           func(context.Context, ...string) error
	ServicePID          func(context.Context, string) (int, error)
	ProcessRunning      func(int) bool
	StateDiskOpenOwners func(context.Context, string) ([]int, error)
}

func DefaultSystem() System {
	return System{Launchctl: launchctl, ServicePID: servicePID, ProcessRunning: processRunning, StateDiskOpenOwners: stateDiskOpenOwners}
}

func launchctl(ctx context.Context, args ...string) error {
	output, err := exec.CommandContext(ctx, "/bin/launchctl", args...).CombinedOutput()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	message := strings.TrimSpace(string(output))
	if strings.Contains(message, "Could not find service") || strings.Contains(message, "Could not find domain") || strings.Contains(message, "Domain does not support specified action") || (args[0] == "bootout" && strings.Contains(message, "No such process")) {
		return fmt.Errorf("%w: %s", ErrNotFound, message)
	}
	return fmt.Errorf("launchctl %s: %w: %s", args[0], err, message)
}

func servicePID(ctx context.Context, target string) (int, error) {
	output, err := exec.CommandContext(ctx, "/bin/launchctl", "print", target).Output()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && strings.TrimSpace(key) == "pid" {
			return strconv.Atoi(strings.TrimSpace(value))
		}
	}
	return 0, nil
}

func processRunning(pid int) bool {
	err := unix.Kill(pid, 0)
	return err == nil || errors.Is(err, unix.EPERM)
}

func stateDiskOpenOwners(ctx context.Context, path string) ([]int, error) {
	if _, err := os.Lstat(path); err == nil {
		output, err := inspectOpenFiles(ctx, "-Fpn", "--", path)
		return openOwners(output, ""), err
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if parent == "" {
		parent = filepath.Dir(path)
	}
	expected := filepath.Join(parent, filepath.Base(path))
	output, err := inspectOpenFiles(ctx, "+L1", "-Fpn")
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("state disk changed during attachment inspection")
	}
	return openOwners(output, expected), nil
}

func openOwners(output []byte, expected string) []int {
	var owners []int
	pid := 0
	for _, line := range strings.Split(string(output), "\n") {
		if len(line) < 2 {
			continue
		}
		switch line[0] {
		case 'p':
			pid, _ = strconv.Atoi(line[1:])
			if expected == "" && pid > 0 {
				owners = append(owners, pid)
			}
		case 'n':
			if expected != "" && strings.TrimSuffix(line[1:], " (deleted)") == expected && pid > 0 {
				owners = append(owners, pid)
			}
		}
	}
	return owners
}

func inspectOpenFiles(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/usr/sbin/lsof", args...)
	var output, diagnostic bytes.Buffer
	command.Stdout = &inspectionOutput{buffer: &output, cancel: cancel}
	command.Stderr = &inspectionOutput{buffer: &diagnostic, cancel: cancel}
	err := command.Run()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 && diagnostic.Len() == 0 {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if diagnostic.Len() != 0 {
		return nil, errors.New("open-file inspection returned a diagnostic")
	}
	return output.Bytes(), nil
}

type inspectionOutput struct {
	buffer *bytes.Buffer
	cancel context.CancelFunc
}

func (w *inspectionOutput) Write(data []byte) (int, error) {
	if w.buffer.Len()+len(data) > 16<<20 {
		w.cancel()
		return 0, io.ErrShortBuffer
	}
	return w.buffer.Write(data)
}

func (system System) WaitStateDiskRelease(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		owners, err := system.StateDiskOpenOwners(ctx, path)
		if err != nil {
			return fmt.Errorf("verify VM state disk ownership: %w", err)
		}
		if len(owners) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("VM state disk is still open by process IDs %v: %w", owners, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func WaitStateDiskRelease(ctx context.Context, path string) error {
	return DefaultSystem().WaitStateDiskRelease(ctx, path)
}

func PrepareDaemonLogs(gid int) error {
	const path = "/Library/Logs/AgentVMM"
	if err := privatefs.ValidateAncestors(path, 0); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return err
	}
	if stat.Uid != 0 {
		return errors.New("daemon log directory must be owned by root")
	}
	if err := unix.Fchown(fd, 0, gid); err != nil {
		return err
	}
	return unix.Fchmod(fd, 0750)
}
