package lifecycle

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestBlockedOperationCLI(t *testing.T) {
	if root := os.Getenv("VMM_BLOCKED_CLI_ROOT"); root != "" {
		err := writeOperationReport(testPaths(root), []string{"--request-id", "old-request", "--registration-id", "unproven-registration"}, os.Stdout)
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	p, request, actions := maintenanceFixture(t)
	original := lifecycleOperation{OperationID: "old-request", RequestID: "old-request", Kind: "install-managed", Stage: "register", Outcome: "failed", FailureCode: "transient_unavailable", Retryable: true}
	if err := persistOperation(p, original); err != nil {
		t.Fatal(err)
	}
	request.DataPolicy, request.ConfirmDataDeletion = "reset", true
	if _, err := runMaintenance(p, request, actions); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestBlockedOperationCLI$")
	command.Env = append(os.Environ(), "VMM_BLOCKED_CLI_ROOT="+filepath.Dir(p.home))
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err == nil {
		t.Fatalf("blocked filtered read succeeded: %s", stdout.String())
	}
	var result struct {
		Error struct {
			Code      string `json:"code"`
			Reason    string `json:"reason"`
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("stdout=%s stderr=%s: %v", stdout.String(), stderr.String(), err)
	}
	if result.Error.Code != "request_blocked" || result.Error.Reason != "local_owner_fence" || result.Error.RequestID != original.RequestID {
		t.Fatalf("negative proof=%+v", result)
	}
	saved, err := readLifecycleOperationByID(p, original.RequestID)
	if err != nil || !reflect.DeepEqual(saved, &original) {
		t.Fatalf("history changed: %+v %v", saved, err)
	}
	if err := persistOperation(p, lifecycleOperation{OperationID: "unbound", RequestID: "unbound", Kind: "install-managed", Outcome: "failed", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	var unknown bytes.Buffer
	if err := writeOperationReport(p, []string{"--request-id", "unbound", "--registration-id", "unproven-registration"}, &unknown); err == nil || unknown.Len() != 0 {
		t.Fatalf("unproven target accepted: %s %v", unknown.String(), err)
	}
}
