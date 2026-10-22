//go:build darwin || linux

package main

import (
	"errors"
	"time"
)

func baseManagedEnvelope(command string, now time.Time, target map[string]any) managedEnvelope {
	return managedEnvelope{Version: managedEnvelopeVersion, Command: command, Target: target, Source: []string{}, ObservedAt: now.Format(time.RFC3339Nano), Freshness: "unknown", State: "unknown", Stage: "unknown", Issue: nil, OperationRef: nil, NextAction: []managedNextAction{}, Facts: map[string]any{}}
}

func targetFromStatus(status map[string]any, opts managedOptions) map[string]any {
	target := explicitManagedTarget(opts)
	target["service_user"] = stringFact(status, "serviceUser")
	if uid := numberFact(status, "serviceUid"); uid != 0 {
		target["service_uid"] = uid
	}
	target["service_job"] = stringFact(status, "hostJob")
	if app := stringFact(status, "serviceApp"); app != "" {
		target["install_location"] = app
	}
	return target
}

func validateManagedStatusTarget(status map[string]any) error {
	if stringFact(status, "serviceUser") == "" || numberFact(status, "serviceUid") <= 0 || stringFact(status, "hostJob") == "" || stringFact(status, "serviceApp") == "" {
		return errors.New("lifecycle status did not identify one service UID, job, and installation location")
	}
	return nil
}

func explicitManagedTarget(opts managedOptions) map[string]any {
	target := map[string]any{}
	for key, value := range map[string]string{"registration_id": opts.registrationID, "environment_id": opts.environmentID, "workload_id": opts.workloadID, "container_id": opts.containerID} {
		if value != "" {
			target[key] = value
		}
	}
	return target
}

func managedState(status map[string]any) string {
	if boolFact(status, "hostHealthy") {
		return "healthy"
	}
	if boolFact(status, "hostReadable") {
		return "unhealthy"
	}
	if boolFact(status, "hostRunning") {
		return "host_unreadable"
	}
	if boolFact(status, "hostLoaded") {
		return "exited"
	}
	if boolFact(status, "hostInstalled") {
		return "stopped"
	}
	return "not_installed"
}

func managedStageAndIssue(status map[string]any) (string, any) {
	if boolFact(status, "hostHealthy") {
		return "healthy", nil
	}
	if startup, ok := status["startup"].(map[string]any); ok {
		stage := firstNonEmptyManaged(stringFact(startup, "failureStage"), stringFact(startup, "currentStage"), "unknown")
		if boolFact(startup, "failureConfirmed") {
			return stage, managedIssue{Code: "startup_failed", Message: boundedManagedText(stringFact(startup, "failureMessage"), 1024), Layer: "guest", Retryable: managedBool(false)}
		}
		return stage, nil
	}
	if issue := stringFact(status, "hostObservationIssue"); issue != "" {
		return "service_observation", managedIssue{Code: "service_observation_failed", Message: boundedManagedText(issue, 1024), Layer: "host", Retryable: managedBool(true)}
	}
	return "unknown", nil
}

func managedFreshness(status map[string]any, now time.Time) string {
	if boolFact(status, "hostReadable") {
		if observed := stringFact(status, "snapshotObservedAt"); observed != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, observed); err == nil {
				return freshnessFromTime(parsed, now)
			}
		}
	}
	if startup, ok := status["startup"].(map[string]any); ok {
		if observed := stringFact(startup, "observedAt"); observed != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, observed); err == nil {
				return freshnessFromTime(parsed, now)
			}
		}
	}
	if observed := stringFact(status, "snapshotObservedAt"); observed != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, observed); err == nil {
			return freshnessFromTime(parsed, now)
		}
	}
	if boolFact(status, "hostObserved") || boolFact(status, "hostReadable") || boolFact(status, "hostHealthy") {
		return "current"
	}
	if operation, ok := status["operation"].(map[string]any); ok {
		return freshnessFromMillis(numberFact(operation, "updatedAt"), now)
	}
	return "unknown"
}

func freshnessFromMillis(value int64, now time.Time) string {
	if value == 0 {
		return "unknown"
	}
	return freshnessFromTime(time.UnixMilli(value), now)
}

func freshnessFromTime(value, now time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	if now.Sub(value) > 30*time.Second {
		return "stale"
	}
	return "current"
}
