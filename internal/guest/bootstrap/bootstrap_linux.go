//go:build linux

package bootstrap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const StateMount = "/state"
const ControlMount = "/control"

type Result struct {
	StateDevice      string
	ControlDevice    string
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
	controlDevice, poolDevice := partitionDevice(stateDevice, 1), partitionDevice(stateDevice, 2)
	if migrationRequested(parameters) {
		legacyDevice, err := deviceByIdentifier("AGENT_STATE_LEGACY")
		if err != nil {
			return nil, err
		}
		report, err := MigrateLegacyState(legacyDevice, stateDevice)
		if err != nil {
			return nil, fmt.Errorf("migrate legacy state: %w", err)
		}
		fmt.Printf("state migration complete: entries=%d logical_bytes=%d\n", report.Entries, report.LogicalBytes)
	} else if parameters["agent.state_init"] == "1" {
		if err := initializeStateDisk(stateDevice); err != nil {
			return nil, err
		}
	}
	for _, filesystem := range []struct{ device, mount string }{{controlDevice, ControlMount}, {poolDevice, StateMount}} {
		if err := os.MkdirAll(filesystem.mount, 0755); err != nil {
			return nil, err
		}
		if err := unix.Mount(filesystem.device, filesystem.mount, "ext4", unix.MS_NOATIME, "prjquota"); err != nil && !errors.Is(err, unix.EBUSY) {
			return nil, fmt.Errorf("mount %s: %w. Existing single-filesystem disks require offline conversion", filesystem.mount, err)
		}
	}
	for _, directory := range []string{"containerd", "volumes", "workspaces", "logs", "cni"} {
		if err := os.MkdirAll(filepath.Join(ControlMount, directory), 0700); err != nil {
			return nil, err
		}
	}
	// The content plugin has no root_path option. Its only persistent root is
	// mounted from the workload filesystem before containerd starts.
	contentSource := filepath.Join(StateMount, "containerd", "content")
	contentTarget := filepath.Join(ControlMount, "containerd", "io.containerd.content.v1.content")
	for _, directory := range []string{contentSource, contentTarget} {
		if err := os.MkdirAll(directory, 0700); err != nil {
			return nil, err
		}
	}
	if err := unix.Mount(contentSource, contentTarget, "", unix.MS_BIND, ""); err != nil {
		return nil, fmt.Errorf("mount content pool: %w", err)
	}
	for _, directory := range []string{"containerd", "volumes", "workspaces", "logs", "build-cache", "build-tmp", "cni"} {
		if err := os.MkdirAll(filepath.Join(StateMount, directory), 0o700); err != nil {
			return nil, err
		}
	}
	temporary := filepath.Join(ControlMount, "tmp")
	if err := os.RemoveAll(temporary); err != nil {
		return nil, err
	}
	if err := os.Mkdir(temporary, 0o1777); err != nil {
		return nil, err
	}
	if err := unix.Mount(temporary, "/tmp", "", unix.MS_BIND, ""); err != nil {
		return nil, fmt.Errorf("mount temporary directory: %w", err)
	}
	if err := unix.Mount(filepath.Join(ControlMount, "cni"), "/var/lib/cni", "", unix.MS_BIND, ""); err != nil {
		return nil, fmt.Errorf("mount CNI state: %w", err)
	}
	return &Result{StateDevice: poolDevice, ControlDevice: controlDevice, StateInitialized: true, RootReadOnly: rootReadOnly()}, nil
}

func partitionDevice(device string, index int) string {
	separator := ""
	if device[len(device)-1] >= '0' && device[len(device)-1] <= '9' {
		separator = "p"
	}
	return fmt.Sprintf("%s%s%d", device, separator, index)
}

func initializeStateDisk(device string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "sfdisk", device)
	command.Stdin = strings.NewReader("label: gpt\nstart=1MiB, size=8GiB, type=L, name=control\ntype=L, name=workloads\n")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("partition state disk: %w: %s", err, output)
	}
	file, err := os.Open(device)
	if err != nil {
		return err
	}
	err = unix.IoctlSetInt(int(file.Fd()), unix.BLKRRPART, 0)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, index := range []int{1, 2} {
		path := partitionDevice(device, index)
		for {
			if _, err := os.Stat(path); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if numbers, err := os.ReadFile(filepath.Join("/sys/class/block", filepath.Base(path), "dev")); err == nil {
				parts := strings.Split(strings.TrimSpace(string(numbers)), ":")
				if len(parts) != 2 {
					return fmt.Errorf("invalid device numbers for %s", path)
				}
				major, majorErr := strconv.ParseUint(parts[0], 10, 32)
				minor, minorErr := strconv.ParseUint(parts[1], 10, 32)
				if majorErr != nil || minorErr != nil {
					return errors.Join(majorErr, minorErr)
				}
				if err := unix.Mknod(path, unix.S_IFBLK|0o600, int(unix.Mkdev(uint32(major), uint32(minor)))); err != nil && !errors.Is(err, unix.EEXIST) {
					return err
				}
				continue
			}
			if !time.Now().Before(deadline) {
				return fmt.Errorf("state partition %s did not appear", path)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, index := range []int{1, 2} {
		command = exec.CommandContext(ctx, "mkfs.ext4", stateFilesystemFormatArgs(partitionDevice(device, index))...)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("format state partition: %w: %s", err, output)
		}
	}
	return nil
}

func stateFilesystemFormatArgs(stateDevice string) []string {
	// The host fully allocates state.raw before boot. mke2fs otherwise issues
	// discard for the whole device and punches those backing-file extents,
	// making the next host-side sparse-file audit reject the state disk.
	return []string{"-F", "-q", "-O", "project,quota", "-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", stateDevice}
}

func UnmountState() error {
	for _, target := range []string{"/var/lib/cni", "/tmp", "/control/containerd/io.containerd.content.v1.content", StateMount, ControlMount} {
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
