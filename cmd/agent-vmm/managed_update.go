//go:build darwin || linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const managedMaxHostArtifactBytes = uint64(4 << 30)

var managedUpdateHTTPClient = &http.Client{Timeout: 10 * time.Minute}

type managedUpdateError struct {
	stage     string
	code      string
	retryable bool
	err       error
}

type managedUpdatePreparationFailure struct {
	Version   int    `json:"version"`
	Stage     string `json:"stage"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (failure managedUpdateError) Error() string { return failure.err.Error() }
func (failure managedUpdateError) Unwrap() error { return failure.err }

func managedUpdateFailure(stage string, err error) error {
	return managedUpdateError{stage: stage, code: "update_" + stage + "_failed", err: err}
}

func managedRetryableUpdateFailure(stage string, err error) error {
	return managedUpdateError{stage: stage, code: "update_" + stage + "_unavailable", retryable: true, err: err}
}

func managedUpdateFailureStage(err error) string {
	var failure managedUpdateError
	if errors.As(err, &failure) {
		return failure.stage
	}
	return "validation"
}

func managedUpdateFailureDetails(err error) managedUpdateError {
	var failure managedUpdateError
	if errors.As(err, &failure) {
		failure.err = errors.New(boundedManagedText(failure.err.Error(), 512))
		return failure
	}
	return managedUpdateError{stage: "preparation", code: "update_preparation_failed", err: errors.New(boundedManagedText(err.Error(), 512))}
}

func updateOperationFromStatus(status map[string]any) map[string]any {
	operation, _ := status["operation"].(map[string]any)
	return operation
}

func updateRetryAt(operation map[string]any) time.Time {
	return time.UnixMilli(numberFact(operation, "updatedAt")).Add(managedUpdateRetryDelay)
}

func applyUpdateRetryDisposition(envelope managedEnvelope, operation map[string]any, opts managedOptions, now time.Time) (managedEnvelope, bool) {
	if len(operation) == 0 {
		return envelope, false
	}
	if stringFact(operation, "kind") != "update" {
		if outcome := stringFact(operation, "outcome"); outcome == "pending" || outcome == "unknown" {
			envelope.State = "transition_in_progress"
			envelope.Stage = firstNonEmptyManaged(stringFact(operation, "stage"), "lifecycle")
			envelope.Disposition = managedDispositionExclusiveTransition
			envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
			envelope.Issue = managedIssue{Code: "lifecycle_transition_in_progress", Message: "a lifecycle transition is already in progress", Layer: "lifecycle", Retryable: managedBool(true)}
			return envelope, true
		}
		return envelope, false
	}
	if stringFact(operation, "requestId") != opts.requestID {
		if outcome := stringFact(operation, "outcome"); outcome == "pending" || outcome == "unknown" {
			envelope.State = "transition_in_progress"
			envelope.Stage = firstNonEmptyManaged(stringFact(operation, "stage"), "lifecycle")
			envelope.Disposition = managedDispositionExclusiveTransition
			envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
			envelope.Issue = managedIssue{Code: "lifecycle_transition_in_progress", Message: "a lifecycle transition is already in progress", Layer: "lifecycle", Retryable: managedBool(true)}
			return envelope, true
		}
		return envelope, false
	}
	if stringFact(operation, "targetReleaseId") != opts.targetRelease {
		envelope.State = "action_required"
		envelope.Stage = "target_selection"
		envelope.Disposition = managedDispositionTerminalActionRequired
		envelope.Issue = managedIssue{Code: "update_target_conflict", Message: "the request identity is already bound to a different release", Layer: "lifecycle", Retryable: managedBool(false)}
		envelope.NextAction = []managedNextAction{{Action: "review_selected_release", Permission: "administrator"}}
		return envelope, true
	}
	if !strings.EqualFold(stringFact(operation, "targetArtifactSha256"), opts.artifactSHA256) ||
		uint64(numberFact(operation, "targetArtifactSize")) != opts.artifactSize {
		envelope.State = "action_required"
		envelope.Stage = "target_selection"
		envelope.Disposition = managedDispositionTerminalActionRequired
		envelope.Issue = managedIssue{Code: "update_target_conflict", Message: "the request identity is already bound to different artifact bytes", Layer: "lifecycle", Retryable: managedBool(false)}
		envelope.NextAction = []managedNextAction{{Action: "review_selected_release", Permission: "administrator"}}
		return envelope, true
	}
	switch stringFact(operation, "outcome") {
	case "succeeded":
		envelope.State = "succeeded"
		envelope.Stage = "complete"
		envelope.Disposition = managedDispositionContinueNormalWork
		envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
		envelope.Facts["already_completed"] = true
		return envelope, true
	case "pending", "unknown":
		envelope.State = "transition_in_progress"
		envelope.Stage = firstNonEmptyManaged(stringFact(operation, "stage"), "lifecycle")
		envelope.Disposition = managedDispositionExclusiveTransition
		envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
		envelope.Issue = managedIssue{Code: "update_transition_in_progress", Message: "the lifecycle owner has not reported a terminal update outcome", Layer: "lifecycle", Retryable: managedBool(true)}
		return envelope, true
	case "failed":
	default:
		return envelope, false
	}
	attempt := numberFact(operation, "attempt")
	envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
	envelope.Stage = firstNonEmptyManaged(stringFact(operation, "failureStage"), stringFact(operation, "stage"), "preparation")
	envelope.Facts["attempt"] = attempt
	envelope.Facts["max_attempts"] = int64(3)
	if !boolFact(operation, "retryable") || attempt >= 3 {
		envelope.State = "action_required"
		envelope.Disposition = managedDispositionTerminalActionRequired
		envelope.Issue = managedIssue{Code: firstNonEmptyManaged(stringFact(operation, "failureCode"), "update_failed"), Message: boundedManagedText(stringFact(operation, "failureMessage"), 512), Layer: "lifecycle", Retryable: managedBool(false)}
		envelope.NextAction = []managedNextAction{{Action: "review_update_failure", Permission: "administrator"}}
		return envelope, true
	}
	retryAt := updateRetryAt(operation)
	if now.Before(retryAt) {
		envelope.State = "retry_wait"
		envelope.Disposition = managedDispositionRetrySameRequestAt
		envelope.Facts["retry_at"] = retryAt.UTC().Format(time.RFC3339Nano)
		envelope.Issue = managedIssue{Code: firstNonEmptyManaged(stringFact(operation, "failureCode"), "update_failed"), Message: boundedManagedText(stringFact(operation, "failureMessage"), 512), Layer: "lifecycle", Retryable: managedBool(true)}
		envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}}
		return envelope, true
	}
	return envelope, false
}

func (runtime managedRuntime) recordUpdatePreparationFailure(opts managedOptions, failure managedUpdateError) (map[string]any, error) {
	descriptor, err := json.Marshal(managedUpdatePreparationFailure{
		Version: 1, Stage: failure.stage, Code: failure.code,
		Message: boundedManagedText(failure.err.Error(), 512), Retryable: failure.retryable,
	})
	if err != nil {
		return nil, err
	}
	args := []string{"update", "--preparation-failure-stdin", "--target-release-id", opts.targetRelease, "--request-id", opts.requestID,
		"--artifact-sha256", strings.ToLower(opts.artifactSHA256), "--artifact-size", fmt.Sprint(opts.artifactSize)}
	args = appendServiceArguments(args, opts)
	recordCtx, cancelRecord := context.WithTimeout(context.Background(), managedInspectBudget)
	stdout, _, runErr := runtime.runHelperInput(recordCtx, opts.lifecycle, args, descriptor)
	cancelRecord()
	var operation map[string]any
	if jsonUnmarshalManaged(stdout, &operation) != nil || stringFact(operation, "requestId") != opts.requestID ||
		stringFact(operation, "kind") != "update" || stringFact(operation, "targetReleaseId") != opts.targetRelease ||
		stringFact(operation, "outcome") != "failed" || !strings.EqualFold(stringFact(operation, "targetArtifactSha256"), opts.artifactSHA256) ||
		uint64(numberFact(operation, "targetArtifactSize")) != opts.artifactSize {
		return nil, errors.New("lifecycle did not durably record the bound update preparation failure")
	}
	verifyArgs := []string{"operation", "--operation-id", stringFact(operation, "operationId")}
	verifyArgs = appendServiceArguments(verifyArgs, opts)
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), managedInspectBudget)
	verifiedOutput, _, verifyErr := runtime.runHelperInput(verifyCtx, opts.lifecycle, verifyArgs, nil)
	cancelVerify()
	var verified map[string]any
	if verifyErr != nil || jsonUnmarshalManaged(verifiedOutput, &verified) != nil ||
		stringFact(verified, "operationId") != stringFact(operation, "operationId") ||
		stringFact(verified, "requestId") != opts.requestID || stringFact(verified, "kind") != "update" ||
		stringFact(verified, "targetReleaseId") != opts.targetRelease ||
		!strings.EqualFold(stringFact(verified, "targetArtifactSha256"), opts.artifactSHA256) ||
		uint64(numberFact(verified, "targetArtifactSize")) != opts.artifactSize ||
		stringFact(verified, "outcome") != "failed" || numberFact(verified, "attempt") != numberFact(operation, "attempt") ||
		numberFact(verified, "updatedAt") != numberFact(operation, "updatedAt") ||
		stringFact(verified, "failureCode") != stringFact(operation, "failureCode") {
		return nil, errors.New("lifecycle did not confirm the durable update preparation failure")
	}
	return operation, runErr
}

func (runtime managedRuntime) update(ctx context.Context, opts managedOptions) (managedEnvelope, error) {
	status, err := runtime.lifecycleStatus(ctx, opts)
	if err != nil {
		return managedEnvelope{}, err
	}
	if err := validateManagedStatusTarget(status); err != nil {
		return managedEnvelope{}, err
	}
	envelope := baseManagedEnvelope("update", runtime.now().UTC(), targetFromStatus(status, opts))
	envelope.Source = []string{"release_target", "lifecycle"}
	envelope.Freshness = "current"
	envelope.State = "preparing"
	envelope.Stage = "target_selection"
	envelope.Facts = map[string]any{
		"request_id":        opts.requestID,
		"target_release_id": opts.targetRelease,
		"artifact_sha256":   strings.ToLower(opts.artifactSHA256),
		"artifact_size":     opts.artifactSize,
		"stages":            []string{"target_selection", "download", "preparation", "stop", "replace", "recovery", "complete"},
	}
	if strings.HasPrefix(stringFact(status, "hostJob"), "system/") {
		envelope.State = "needs_administrator"
		envelope.Stage = "target_selection"
		envelope.Disposition = managedDispositionAdministratorAction
		envelope.Issue = managedIssue{Code: "administrator_update_required", Message: "the daemon Host update requires the administrator installer and fixed service executor", Layer: "service", Retryable: managedBool(true)}
		envelope.NextAction = []managedNextAction{{Action: "administrator_update", Permission: "administrator"}}
		return envelope, nil
	}
	if issue, permission, refused := managedPrivilegeIssue("update", os.Geteuid()); refused {
		envelope.State = issue.Code
		envelope.Disposition = managedDispositionAdministratorAction
		envelope.Issue = issue
		envelope.NextAction = []managedNextAction{{Action: "update", Permission: permission}}
		return envelope, nil
	}
	if handled, done := applyUpdateRetryDisposition(envelope, updateOperationFromStatus(status), opts, runtime.now().UTC()); done {
		return handled, nil
	}
	stagedApp, err := runtime.prepareUpdate(ctx, opts, status)
	if err != nil {
		failure := managedUpdateFailureDetails(err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			failure = managedUpdateError{stage: failure.stage, code: "update_preparation_cancelled", retryable: true, err: errors.New("the update download or preparation was cancelled; the current installation was not changed")}
		}
		operation, recordErr := runtime.recordUpdatePreparationFailure(opts, failure)
		if recordErr != nil && len(operation) == 0 {
			envelope.State = "action_required"
			envelope.Stage = failure.stage
			envelope.Disposition = managedDispositionTerminalActionRequired
			envelope.Issue = managedIssue{Code: "update_failure_record_unknown", Message: "the selected artifact was not installed and its retry state could not be recorded", Layer: "lifecycle", Retryable: managedBool(false)}
			envelope.NextAction = []managedNextAction{{Action: "review_update_failure", Permission: "administrator"}}
			return envelope, nil
		}
		handled, _ := applyUpdateRetryDisposition(envelope, operation, opts, runtime.now().UTC())
		return handled, nil
	}
	if stagedApp == "" {
		envelope.State = "succeeded"
		envelope.Stage = "complete"
		envelope.Disposition = managedDispositionContinueNormalWork
		envelope.Facts["already_current"] = true
		return envelope, nil
	}
	envelope.Stage = "preparation"
	args := []string{"update", "--source-app", stagedApp, "--target-release-id", opts.targetRelease, "--request-id", opts.requestID,
		"--artifact-sha256", strings.ToLower(opts.artifactSHA256), "--artifact-size", fmt.Sprint(opts.artifactSize)}
	args = appendServiceArguments(args, opts)
	stdout, _, runErr := runtime.runHelper(ctx, opts.lifecycle, args)
	var operation map[string]any
	decoded := jsonUnmarshalManaged(stdout, &operation) == nil && stringFact(operation, "requestId") == opts.requestID &&
		stringFact(operation, "kind") == "update" && stringFact(operation, "targetReleaseId") == opts.targetRelease &&
		strings.EqualFold(stringFact(operation, "targetArtifactSha256"), opts.artifactSHA256) &&
		uint64(numberFact(operation, "targetArtifactSize")) == opts.artifactSize
	if runErr != nil && !decoded {
		envelope.State = "transition_in_progress"
		envelope.Stage = "owner_response_pending"
		envelope.Disposition = managedDispositionExclusiveTransition
		envelope.Partial = true
		envelope.Issue = managedIssue{Code: "unknown_outcome", Message: "the lifecycle owner may have accepted the update; retry only the same request id and selected release", Layer: "lifecycle", Retryable: managedBool(true)}
		envelope.NextAction = []managedNextAction{{Action: "retry_same_request_id", Permission: "service_user"}}
		envelope.Facts["request_id"] = opts.requestID
		envelope.Facts["prepared_app"] = stagedApp
		return envelope, nil
	}
	if !decoded {
		return managedEnvelope{}, errors.New("lifecycle returned an unbound update operation")
	}
	if stringFact(operation, "outcome") == "succeeded" {
		if err := runtime.removePrepared(stagedApp); err != nil && !errors.Is(err, os.ErrNotExist) {
			return managedEnvelope{}, errors.New("retired Host bundle could not be removed after successful update")
		}
	}
	if handled, done := applyUpdateRetryDisposition(envelope, operation, opts, runtime.now().UTC()); done {
		return handled, nil
	}
	envelope.State = stringFact(operation, "outcome")
	envelope.Stage = firstNonEmptyManaged(stringFact(operation, "stage"), "preparation")
	envelope.OperationRef = map[string]any{"owner": "lifecycle", "id": stringFact(operation, "operationId")}
	envelope.Disposition = managedDispositionContinueNormalWork
	return envelope, nil
}

func prepareManagedUpdate(ctx context.Context, opts managedOptions, status map[string]any) (string, error) {
	if opts.artifactSize == 0 || opts.artifactSize > managedMaxHostArtifactBytes {
		return "", managedUpdateFailure("target_selection", errors.New("Host artifact size is outside the managed update bound"))
	}
	parsed, err := url.Parse(opts.artifactURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return "", managedUpdateFailure("target_selection", errors.New("Host artifact URL must be exact HTTPS without credentials, query, or fragment"))
	}
	targetApp := filepath.Clean(stringFact(status, "serviceApp"))
	if !filepath.IsAbs(targetApp) || filepath.Base(targetApp) != managedServiceAppName {
		return "", managedUpdateFailure("target_selection", errors.New("installed Host target is invalid"))
	}
	stagedApp := managedPreparedUpdatePath(targetApp, opts.requestID)
	if current, currentErr := managedHostReleaseID(targetApp, false); currentErr == nil && current == opts.targetRelease {
		if _, statErr := os.Lstat(stagedApp); statErr == nil {
			return stagedApp, nil
		}
		return "", nil
	}
	if _, statErr := os.Lstat(stagedApp); statErr == nil {
		if release, releaseErr := managedHostReleaseID(stagedApp, true); releaseErr == nil && release == opts.targetRelease {
			return stagedApp, nil
		}
		return "", managedUpdateFailure("preparation", errors.New("prepared update path already exists with a different release"))
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", managedUpdateFailure("preparation", errors.New("prepared update path is not readable"))
	}
	archive, err := os.CreateTemp(filepath.Dir(targetApp), ".agent-vmm-update-*.archive")
	if err != nil {
		return "", managedUpdateFailure("download", err)
	}
	archivePath := archive.Name()
	defer os.Remove(archivePath)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		archive.Close()
		return "", managedUpdateFailure("download", err)
	}
	request.Header.Set("User-Agent", "agent-vmm-managed-updater/1")
	response, err := managedUpdateHTTPClient.Do(request)
	if err != nil {
		archive.Close()
		return "", managedRetryableUpdateFailure("download", err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		archive.Close()
		if response.StatusCode >= 500 && response.StatusCode <= 599 {
			return "", managedRetryableUpdateFailure("download", fmt.Errorf("Host artifact download returned HTTP %d", response.StatusCode))
		}
		return "", managedUpdateFailure("download", fmt.Errorf("Host artifact download returned HTTP %d", response.StatusCode))
	}
	if response.ContentLength >= 0 && uint64(response.ContentLength) != opts.artifactSize {
		response.Body.Close()
		archive.Close()
		return "", managedUpdateFailure("download", errors.New("Host artifact Content-Length does not match the selected target"))
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(archive, hash), io.LimitReader(response.Body, int64(opts.artifactSize)+1))
	closeBodyErr := response.Body.Close()
	closeArchiveErr := archive.Close()
	if copyErr != nil || closeBodyErr != nil || closeArchiveErr != nil {
		return "", managedRetryableUpdateFailure("download", errors.New("Host artifact download did not complete"))
	}
	if uint64(written) != opts.artifactSize || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), opts.artifactSHA256) {
		return "", managedUpdateFailure("download", errors.New("Host artifact bytes do not match the selected size and SHA-256"))
	}
	if err := extractManagedUpdate(ctx, archivePath, stagedApp, opts.targetRelease); err != nil {
		return "", err
	}
	return stagedApp, nil
}
