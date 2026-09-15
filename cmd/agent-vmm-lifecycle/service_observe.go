package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type launchctlServiceFacts struct {
	Loaded         bool
	Running        bool
	PID            int
	LastExitStatus *int
}

var launchctlServiceObserver = launchctlServiceObservation

func launchctlServiceObservation(ctx context.Context, target string) (launchctlServiceFacts, error) {
	command := exec.CommandContext(ctx, "/bin/launchctl", "print", target)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if launchctlServiceAbsent("print", message) {
			return launchctlServiceFacts{}, fmt.Errorf("%w: %s", errLaunchctlServiceNotFound, message)
		}
		return launchctlServiceFacts{}, fmt.Errorf("launchctl print failed: %w", err)
	}
	return parseLaunchctlServiceObservation(output)
}

func parseLaunchctlServiceObservation(output []byte) (launchctlServiceFacts, error) {
	result := launchctlServiceFacts{Loaded: true}
	stateSeen := false
	for _, line := range strings.Split(string(output), "\n") {
		key, raw, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		key, raw = strings.TrimSpace(key), strings.TrimSpace(raw)
		switch key {
		case "state":
			stateSeen = true
			result.Running = raw == "running"
		case "pid":
			pid, err := strconv.Atoi(raw)
			if err != nil || pid <= 0 {
				return launchctlServiceFacts{}, errors.New("launchctl print reported an invalid Host PID")
			}
			result.PID = pid
		case "last exit code", "last exit status":
			status, err := strconv.Atoi(raw)
			if err == nil {
				result.LastExitStatus = &status
			}
		}
	}
	if !stateSeen {
		return launchctlServiceFacts{}, errors.New("launchctl print did not report Host state")
	}
	if result.Running && result.PID == 0 {
		return launchctlServiceFacts{}, errors.New("launchctl print reported a running Host without a PID")
	}
	if result.PID > 0 && !processRunning(result.PID) {
		result.Running = false
	}
	return result, nil
}
