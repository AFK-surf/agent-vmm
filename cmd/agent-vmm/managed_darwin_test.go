package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestManagedRepairReturnsAdministratorActionForExitedSystemJob(t *testing.T) {
	calls := 0
	runtime := managedRuntime{runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
		calls++
		if args[0] != "status" {
			t.Fatalf("unexpected mutation: %v", args)
		}
		return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"system/com.afk-surf.agent-vmm-host","hostInstalled":true,"hostObserved":true,"hostLoaded":true,"hostRunning":false}`), nil, nil
	}}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"repair", "--request-id", "request-1", "--json", "--lifecycle-helper", "/tmp/lifecycle"}, &stdout, &stderr, runtime)
	if exit != 0 || calls != 1 || !strings.Contains(stdout.String(), `"state":"needs_administrator"`) || !strings.Contains(stdout.String(), `"action":"service_start"`) {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", exit, calls, stdout.String(), stderr.String())
	}
}

func TestManagedRepairTransportLossReturnsSameTargetUnknownOutcome(t *testing.T) {
	calls := 0
	runtime := managedRuntime{now: time.Now, runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
		calls++
		if args[0] == "status" {
			return []byte(`{"serviceApp":"/Applications/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","hostRunning":true}`), nil, nil
		}
		return nil, []byte("connection lost"), context.DeadlineExceeded
	}}
	envelope, err := runtime.repair(context.Background(), managedOptions{requestID: "request-1", registrationID: "registration-1", lifecycle: "/tmp/lifecycle"})
	if err != nil || calls != 2 || envelope.State != "transition_in_progress" || envelope.Facts["request_id"] != "request-1" || envelope.NextAction[0].Action != "retry_same_request_id" {
		t.Fatalf("envelope=%+v calls=%d err=%v", envelope, calls, err)
	}
}

func TestManagedUpdateKeepsPreparationSeparateFromReplacement(t *testing.T) {
	helperCalls := 0
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			helperCalls++
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			t.Fatalf("replacement ran after preparation failure: %v", args)
			return nil, nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			return "", errors.New("archive is incomplete")
		},
		runHelperInput: func(_ context.Context, _ string, args []string, input []byte) ([]byte, []byte, error) {
			helperCalls++
			recorded := []byte(`{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("a", 64) + `","targetArtifactSize":1024,"stage":"preparation","outcome":"failed","attempt":1,"failureStage":"preparation","failureCode":"update_preparation_failed","failureMessage":"archive is incomplete","retryable":false,"updatedAt":1789372800000}`)
			if args[0] == "operation" {
				return recorded, nil, nil
			}
			if !strings.Contains(strings.Join(args, " "), "update --preparation-failure-stdin") || !strings.Contains(string(input), `"retryable":false`) {
				t.Fatalf("preparation failure was not recorded: args=%v input=%s", args, input)
			}
			return recorded, nil, errors.New("exit status 1")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("a", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || helperCalls != 2 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"disposition":"terminal_action_required"`) || !strings.Contains(stdout.String(), `"stage":"preparation"`) {
		t.Fatalf("exit=%d helperCalls=%d stdout=%s stderr=%s", exit, helperCalls, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateCancellationReportsUnchangedCurrentInstallation(t *testing.T) {
	helperCalls := 0
	runtime := managedRuntime{
		now: func() time.Time { return time.UnixMilli(1789372800000) },
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			helperCalls++
			if args[0] != "status" {
				t.Fatalf("replacement ran after cancellation: %v", args)
			}
			return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		prepareUpdate: func(ctx context.Context, _ managedOptions, _ map[string]any) (string, error) {
			if deadline, ok := ctx.Deadline(); ok {
				t.Fatalf("preparation inherited an observation deadline: %v", deadline)
			}
			return "", managedUpdateFailure("download", context.Canceled)
		},
		runHelperInput: func(_ context.Context, _ string, args []string, input []byte) ([]byte, []byte, error) {
			helperCalls++
			recorded := []byte(`{"operationId":"operation-cancel","requestId":"update-cancel","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("d", 64) + `","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":1,"failureStage":"download","failureCode":"update_preparation_cancelled","failureMessage":"the update download or preparation was cancelled; the current installation was not changed","retryable":true,"updatedAt":1789372800000}`)
			if args[0] == "operation" {
				return recorded, nil, nil
			}
			if !strings.Contains(strings.Join(args, " "), "--preparation-failure-stdin") || !strings.Contains(string(input), `"retryable":true`) {
				t.Fatalf("cancelled preparation was not recorded: args=%v input=%s", args, input)
			}
			return recorded, nil, errors.New("exit status 1")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-cancel", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("d", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || helperCalls != 2 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"state":"retry_wait"`) || !strings.Contains(stdout.String(), `"disposition":"retry_same_request_at"`) || !strings.Contains(stdout.String(), "current installation was not changed") {
		t.Fatalf("exit=%d helperCalls=%d stdout=%s stderr=%s", exit, helperCalls, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateDoesNotInferDurabilityFromFailedHelperOutput(t *testing.T) {
	digest := strings.Repeat("f", 64)
	helperCalls := 0
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] != "status" {
				t.Fatalf("unexpected helper call: %v", args)
			}
			return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			return "", managedRetryableUpdateFailure(errors.New("network unavailable"))
		},
		runHelperInput: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, []byte, error) {
			helperCalls++
			if args[0] != "host-update" {
				t.Fatalf("unexpected failure readback: %v", args)
			}
			return []byte(`{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","outcome":"unknown","failureCode":"update_download_unavailable","retryable":true}`), nil, errors.New("persistence failed")
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", digest, "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || helperCalls != 1 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "update_failure_record_unknown") || strings.Contains(stdout.String(), `"state":"retry_wait"`) {
		t.Fatalf("exit=%d calls=%d stdout=%s stderr=%s", exit, helperCalls, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateUsesStableOwnerOperation(t *testing.T) {
	prepared := "/Users/runner/Library/Application Support/Agent VMM Host/current/.Agent VMM Host.update-1.app"
	removed := ""
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			if strings.Join(args, " ") != "host-update --source-app "+prepared+" --target-release-id release-2 --request-id update-1 --artifact-sha256 "+strings.Repeat("b", 64)+" --artifact-size 1024" {
				t.Fatalf("update args=%v", args)
			}
			return []byte(`{"operationId":"operation-2","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"` + strings.Repeat("b", 64) + `","targetArtifactSize":1024,"stage":"complete","outcome":"succeeded"}`), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) { return prepared, nil },
		removePrepared: func(path string) error {
			removed = path
			return nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("b", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || removed != prepared || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"stage":"complete"`) || !strings.Contains(stdout.String(), `"disposition":"continue_normal_work"`) || !strings.Contains(stdout.String(), `"id":"operation-2"`) {
		t.Fatalf("exit=%d removed=%q stdout=%s stderr=%s", exit, removed, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateRejectsLifecycleResultForDifferentRelease(t *testing.T) {
	prepared := "/Users/runner/Library/Application Support/Agent VMM Host/current/.Agent VMM Host.update-1.app"
	removed := false
	runtime := managedRuntime{
		now: time.Now,
		runHelper: func(_ context.Context, _ string, args []string) ([]byte, []byte, error) {
			if args[0] == "status" {
				return []byte(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host"}`), nil, nil
			}
			return []byte(`{"operationId":"operation-other","requestId":"update-1","kind":"update","targetReleaseId":"release-other","stage":"complete","outcome":"succeeded"}`), nil, nil
		},
		prepareUpdate:  func(context.Context, managedOptions, map[string]any) (string, error) { return prepared, nil },
		removePrepared: func(string) error { removed = true; return nil },
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", strings.Repeat("e", 64), "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 1 || removed || !strings.Contains(stderr.String(), "unbound update operation") {
		t.Fatalf("exit=%d removed=%t stdout=%s stderr=%s", exit, removed, stdout.String(), stderr.String())
	}
}

func TestManagedUpdatePreflightHonorsDurableRetryCooldown(t *testing.T) {
	now := time.UnixMilli(1789372800000)
	prepared := false
	digest := strings.Repeat("a", 64)
	runtime := managedRuntime{
		now: func() time.Time { return now },
		runHelper: func(context.Context, string, []string) ([]byte, []byte, error) {
			return []byte(fmt.Sprintf(`{"serviceApp":"/Users/runner/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app","serviceUser":"runner","serviceUid":501,"hostJob":"gui/501/com.afk-surf.agent-vmm-host","operation":{"operationId":"operation-1","requestId":"update-1","kind":"update","targetReleaseId":"release-2","targetArtifactSha256":"%s","targetArtifactSize":1024,"stage":"download","outcome":"failed","attempt":1,"failureStage":"download","failureCode":"update_download_unavailable","failureMessage":"network unavailable","retryable":true,"updatedAt":%d}}`, digest, now.Add(-10*time.Second).UnixMilli())), nil, nil
		},
		prepareUpdate: func(context.Context, managedOptions, map[string]any) (string, error) {
			prepared = true
			return "", nil
		},
	}
	var stdout, stderr bytes.Buffer
	exit := runManagedCLI([]string{"update", "--request-id", "update-1", "--target-release-id", "release-2", "--artifact-url", "https://example.test/Agent-VMM-Host.zip", "--artifact-sha256", digest, "--artifact-size", "1024", "--lifecycle-helper", "/tmp/lifecycle", "--json"}, &stdout, &stderr, runtime)
	if exit != 0 || prepared || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"state":"retry_wait"`) || !strings.Contains(stdout.String(), `"retry_at":"`) {
		t.Fatalf("exit=%d prepared=%t stdout=%s stderr=%s", exit, prepared, stdout.String(), stderr.String())
	}
}

func TestManagedUpdateContinuesAfterPreviousRetryLimit(t *testing.T) {
	now := time.UnixMilli(1789372800000)
	operation := map[string]any{"operationId": "receipt", "requestId": "update-1", "kind": "update", "targetReleaseId": "release-2", "targetArtifactSha256": strings.Repeat("b", 64), "targetArtifactSize": float64(1024), "outcome": "failed", "attempt": float64(5), "failureCode": "update_retry_limit_reached", "retryable": false, "updatedAt": float64(now.Add(-time.Minute).UnixMilli())}
	options := managedOptions{requestID: "update-1", targetRelease: "release-2", artifactSHA256: strings.Repeat("b", 64), artifactSize: 1024}
	_, handled := applyUpdateRetryDisposition(managedEnvelope{Facts: map[string]any{}}, operation, options, now)
	if handled {
		t.Fatal("old lifetime retry count prevented the same request from continuing")
	}
}
