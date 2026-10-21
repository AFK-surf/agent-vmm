package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

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
	home, err := os.UserHomeDir()
	if err != nil {
		fatal("preflight cannot resolve the home directory")
	}
	report, err := preflight(filepath.Join(home, "Library", "Application Support", "AgentVMM"), hostdisk.NewInstallationBytes())
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
