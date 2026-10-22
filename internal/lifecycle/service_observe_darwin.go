package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AFK-surf/agent-vmm/internal/bundle"
)

var launchctlServiceObserver = launchctlServiceObservation

func observeHostService(ctx context.Context, value paths) (hostServiceFacts, error) {
	return launchctlServiceObserver(ctx, value.hostJob())
}

func (value paths) hostJob() string { return value.serviceDomain() + "/" + hostLabel }

func (value paths) hostInstalled() bool {
	return regular(filepath.Join(value.app, filepath.FromSlash(bundle.HostExecutableRelativePath))) && regular(value.hostPlist)
}

func launchctlServiceObservation(ctx context.Context, target string) (hostServiceFacts, error) {
	command := exec.CommandContext(ctx, "/bin/launchctl", "print", target)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if launchctlServiceAbsent("print", message) {
			return hostServiceFacts{}, fmt.Errorf("%w: %s", errHostServiceNotFound, message)
		}
		return hostServiceFacts{}, fmt.Errorf("launchctl print failed: %w", err)
	}
	return parseLaunchctlServiceObservation(output)
}

func parseLaunchctlServiceObservation(output []byte) (hostServiceFacts, error) {
	result := hostServiceFacts{Loaded: true}
	for _, line := range strings.Split(string(output), "\n") {
		key, raw, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		key, raw = strings.TrimSpace(key), strings.TrimSpace(raw)
		switch key {
		case "pid":
			pid, err := strconv.Atoi(raw)
			if err != nil || pid <= 0 {
				return hostServiceFacts{}, errors.New("launchctl print reported an invalid Host PID")
			}
			result.PID = pid
		case "last exit code", "last exit status":
			status, err := strconv.Atoi(raw)
			if err == nil {
				result.LastExitStatus = &status
			}
		}
	}
	if result.PID > 0 && !processRunning(result.PID) {
		result.PID = 0
	}
	result.Running = result.PID > 0
	return result, nil
}
