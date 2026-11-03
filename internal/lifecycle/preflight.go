//go:build darwin || linux

package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/hostdisk"
	"golang.org/x/sys/unix"
)

type preflightIssue struct {
	Code           string `json:"code"`
	RequiredBytes  int64  `json:"requiredBytes"`
	AvailableBytes int64  `json:"availableBytes"`
}

type preflightReport struct {
	OK     bool             `json:"ok"`
	Issues []preflightIssue `json:"issues"`
}

// executePreflight checks a new installation before install. It needs no Host;
// a failed check is a report, not a command error.
func executePreflight(args []string) {
	if len(args) != 1 || args[0] != "--json" {
		fatal("preflight requires --json")
	}
	directory, err := preflightDataDirectory()
	if err != nil {
		fatal(err.Error())
	}
	required, err := preflightBackingBytes(directory)
	if err != nil {
		fatal("preflight cannot read existing backing: " + err.Error())
	}
	report, err := preflight(directory, required)
	if err != nil {
		fatal("preflight cannot read free disk space: " + err.Error())
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		fatal("preflight encoding failed")
	}
}

// preflight measures the volume of the state disk directory, or of its
// nearest existing parent before the first install.
func preflight(directory string, requiredBytes int64) (preflightReport, error) {
	var stats unix.Statfs_t
	for {
		err := unix.Statfs(directory, &stats)
		if err == nil {
			break
		}
		parent := filepath.Dir(directory)
		if !errors.Is(err, unix.ENOENT) || parent == directory {
			return preflightReport{}, err
		}
		directory = parent
	}
	available := int64(min(stats.Bavail*uint64(stats.Bsize), uint64(1<<63-1)))
	report := preflightReport{OK: available >= requiredBytes, Issues: []preflightIssue{}}
	if !report.OK {
		report.Issues = append(report.Issues, preflightIssue{Code: "insufficient_disk", RequiredBytes: requiredBytes, AvailableBytes: available})
	}
	return report, nil
}

func preflightBackingBytes(directory string) (int64, error) {
	path := filepath.Join(directory, "instance", "state.raw")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return hostdisk.NewInstallationBytes(), nil
	}
	if err != nil {
		return 0, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Size() <= 0 || stat.Blocks < 0 {
		return 0, errors.New("existing state backing is invalid")
	}
	allocated := min(uint64(stat.Blocks)*512, uint64(info.Size()))
	missing := uint64(info.Size()) - allocated
	if missing == 0 {
		return 0, nil
	}
	if missing > uint64(1<<63-1-hostdisk.EmergencyBytes) {
		return 0, errors.New("existing state backing exceeds preflight budget")
	}
	return int64(missing) + hostdisk.EmergencyBytes, nil
}
