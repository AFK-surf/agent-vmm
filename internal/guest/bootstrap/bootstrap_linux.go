//go:build linux

package bootstrap

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const StateMount = "/state"

type Result struct {
	StateDevice      string
	StateInitialized bool
	RootReadOnly     bool
}

func Prepare() (*Result, error) {
	commandLine, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return nil, err
	}
	parameters := parseCommandLine(string(commandLine))
	stateDevice, err := deviceByIdentifier("AGENT_STATE")
	if err != nil {
		return nil, err
	}
	if parameters["agent.state_init"] == "1" {
		command := exec.Command("/sbin/mkfs.ext4", "-F", "-q", "-O", "project,quota", "-E", "lazy_itable_init=0,lazy_journal_init=0", stateDevice)
		if output, err := command.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("format state: %w: %s", err, output)
		}
	}
	if err := os.MkdirAll(StateMount, 0o755); err != nil {
		return nil, err
	}
	if err := unix.Mount(stateDevice, StateMount, "ext4", unix.MS_NOATIME, "prjquota"); err != nil && !errors.Is(err, unix.EBUSY) {
		return nil, fmt.Errorf("mount state: %w", err)
	}
	for _, directory := range []string{"containerd", "volumes", "workspaces", "logs", "build-cache", "build-tmp", "cni"} {
		if err := os.MkdirAll(filepath.Join(StateMount, directory), 0o700); err != nil {
			return nil, err
		}
	}
	temporary := filepath.Join(StateMount, "tmp")
	if err := os.RemoveAll(temporary); err != nil {
		return nil, err
	}
	if err := os.Mkdir(temporary, 0o1777); err != nil {
		return nil, err
	}
	if err := unix.Mount(temporary, "/tmp", "", unix.MS_BIND, ""); err != nil {
		return nil, fmt.Errorf("mount temporary directory: %w", err)
	}
	if err := unix.Mount(filepath.Join(StateMount, "cni"), "/var/lib/cni", "", unix.MS_BIND, ""); err != nil {
		return nil, fmt.Errorf("mount CNI state: %w", err)
	}
	return &Result{StateDevice: stateDevice, StateInitialized: true, RootReadOnly: rootReadOnly()}, nil
}

func UnmountState() error {
	for _, target := range []string{"/var/lib/cni", "/tmp", StateMount} {
		if err := unix.Unmount(target, 0); err != nil {
			return fmt.Errorf("unmount %s: %w", target, err)
		}
	}
	return nil
}

func parseCommandLine(value string) map[string]string {
	result := map[string]string{}
	for _, field := range strings.Fields(value) {
		key, item, ok := strings.Cut(field, "=")
		if ok {
			result[key] = item
		}
	}
	return result
}

func deviceByIdentifier(identifier string) (string, error) {
	serials, err := filepath.Glob("/sys/block/vd*/serial")
	if err != nil {
		return "", err
	}
	for _, serial := range serials {
		data, err := os.ReadFile(serial)
		if err == nil && strings.TrimSpace(strings.Trim(string(data), "\x00")) == identifier {
			return "/dev/" + filepath.Base(filepath.Dir(serial)), nil
		}
	}
	return "", fmt.Errorf("block device %q not found", identifier)
}

func rootReadOnly() bool {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 4 && fields[1] == "/" {
			return strings.Contains(","+fields[3]+",", ",ro,")
		}
	}
	return false
}
