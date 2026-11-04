package lifecycle

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func legacyFixture(t *testing.T, outcome string) paths {
	t.Helper()
	p := testPaths(t.TempDir())
	original := lifecycleOperation{OperationID: "legacy-request", RequestID: "legacy-request", Kind: "install-managed", Outcome: outcome, Retryable: outcome == "failed"}
	if err := persistOperation(p, original); err != nil {
		t.Fatal(err)
	}
	var name string
	var raw []byte
	if outcome == "succeeded" {
		name = filepath.Join(p.applicationData, "install-completions", "legacy-request.json")
		raw = []byte(`{"version":1,"operation_id":"legacy-request","registration_id":"legacy-registration"}`)
	} else {
		name = filepath.Join(p.applicationData, "install-operations", "legacy-request", "plan.json")
		raw = []byte(`{"version":1,"operation_id":"legacy-request","material":{"version":1,"operation_id":"legacy-request","registration_id":"legacy-registration"}}`)
	}
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := persistOperation(p, lifecycleOperation{OperationID: "later-update", RequestID: "later-update", Kind: "update", Outcome: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	d, err := openExistingApplicationDataDirectory(p)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, name := range []string{"state.json", maintenanceReceiptName("legacy-request")} {
		var raw map[string]json.RawMessage
		if err := readMaintenanceJSON(d, name, &raw); err != nil {
			t.Fatal(err)
		}
		raw["futureInteger"] = json.RawMessage(`18446744073709551615`)
		if err := writeMaintenanceJSON(d, name, raw); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestLegacyUpgradeCLI(t *testing.T) {
	if root := os.Getenv("VMM_LEGACY_CLI_ROOT"); root != "" {
		if err := writeOperationReport(testPaths(root), []string{"--request-id", "legacy-request", "--registration-id", "legacy-registration"}, os.Stdout); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	for _, outcome := range []string{"failed", "succeeded"} {
		t.Run(outcome, func(t *testing.T) {
			p := legacyFixture(t, outcome)
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyUpgradeCLI$")
			cmd.Env = append(os.Environ(), "VMM_LEGACY_CLI_ROOT="+filepath.Dir(p.home))
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("legacy read: %s %v", output, err)
			}
			var report map[string]json.RawMessage
			if err := json.Unmarshal(output, &report); err != nil {
				t.Fatal(err)
			}
			if string(report["targetRegistrationId"]) != `"legacy-registration"` || string(report["outcome"]) != `"`+outcome+`"` || string(report["kind"]) != `"install-managed"` {
				t.Fatalf("report=%s", output)
			}
			d, err := openExistingApplicationDataDirectory(p)
			if err != nil {
				t.Fatal(err)
			}
			state, err := loadMaintenanceState(d)
			if err != nil {
				t.Fatal(err)
			}
			if state.LatestRequest != "later-update" || state.ActiveRequest != "" || state.RegistrationRequests["legacy-registration"] != "legacy-request" {
				t.Fatalf("owner pointers changed: %+v", state)
			}
			for _, name := range []string{"state.json", maintenanceReceiptName("legacy-request")} {
				var raw map[string]json.RawMessage
				if err := readMaintenanceJSON(d, name, &raw); err != nil {
					t.Fatal(err)
				}
				if string(raw["futureInteger"]) != "18446744073709551615" {
					t.Fatalf("unknown integer changed in %s", name)
				}
			}
			// A receipt committed before its index is repaired by the same bounded read.
			var rawState map[string]json.RawMessage
			if err := readMaintenanceJSON(d, "state.json", &rawState); err != nil {
				t.Fatal(err)
			}
			delete(rawState, "registrationRequests")
			if err := writeMaintenanceJSON(d, "state.json", rawState); err != nil {
				t.Fatal(err)
			}
			d.Close()
			if _, err := readCorrelatedOperation(p, "legacy-request", "legacy-registration"); err != nil {
				t.Fatal(err)
			}
			d, err = openExistingApplicationDataDirectory(p)
			if err != nil {
				t.Fatal(err)
			}
			repaired, err := loadMaintenanceState(d)
			d.Close()
			if err != nil || repaired.RegistrationRequests["legacy-registration"] != "legacy-request" {
				t.Fatalf("partial migration not repaired: %+v %v", repaired, err)
			}
			if outcome == "succeeded" {
				return
			}
			canonical, err := originalRegistrationRequestID(p, "legacy-registration", "explicit-B", "registration-recover")
			if err != nil || canonical != "legacy-request" {
				t.Fatalf("canonical=%s %v", canonical, err)
			}
			p.correlationRequestID = "explicit-B"
			calls := 0
			accepted, err := runLifecycleOperationWithFailedRetry(p, "registration-recover", canonical, "legacy-registration", "", "", 0, true, func() error { calls++; return nil })
			if err != nil || calls != 1 || accepted.RequestID != "legacy-request" || accepted.OperationID != "legacy-request" || accepted.CorrelationRequestID != "explicit-B" {
				t.Fatalf("recovery=%+v calls=%d err=%v", accepted, calls, err)
			}
			// The mutation reply is lost. The native committed correlation still resolves B.
			replay, err := readCorrelatedOperation(p, "explicit-B", "legacy-registration")
			if err != nil || replay["requestId"] != "legacy-request" || replay["outcome"] != "succeeded" {
				t.Fatalf("lost reply=%v %v", replay, err)
			}
			if other, err := readCorrelatedOperation(p, "unaccepted-C", "legacy-registration"); err != nil || other != nil {
				t.Fatalf("other request adopted success: %v %v", other, err)
			}
		})
	}
}

func TestLegacyProofAndFencesRemainFailClosed(t *testing.T) {
	for _, invalid := range []string{
		`{}`, `{"version":2,"operation_id":"legacy-request","material":{"version":1,"operation_id":"legacy-request","registration_id":"legacy-registration"}}`,
		`{"version":1,"operation_id":"wrong","material":{"version":1,"operation_id":"legacy-request","registration_id":"legacy-registration"}}`,
		`{"version":1,"operation_id":"legacy-request","material":{"version":1,"operation_id":"wrong","registration_id":"legacy-registration"}}`,
		`{"version":1,"operation_id":"legacy-request","material":{"version":1,"operation_id":"legacy-request","registration_id":"other-registration"}}`,
	} {
		p := legacyFixture(t, "failed")
		if err := os.WriteFile(filepath.Join(p.applicationData, "install-operations", "legacy-request", "plan.json"), []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		assertLegacyReadUnchanged(t, p, false)
	}
	for _, fence := range []string{"blocked", "newer", "removed", "superseded"} {
		t.Run(fence, func(t *testing.T) {
			p := legacyFixture(t, "failed")
			d, err := openExistingApplicationDataDirectory(p)
			if err != nil {
				t.Fatal(err)
			}
			state, err := loadMaintenanceState(d)
			if err != nil {
				t.Fatal(err)
			}
			switch fence {
			case "blocked":
				state.BlockedInstalls["legacy-request"] = true
			case "newer":
				state.RegistrationRequests["legacy-registration"] = "new-owner"
			case "removed":
				state.RemovedRegistrations["legacy-registration"] = true
			case "superseded":
				state.SupersededRequests["legacy-request"] = "next"
			}
			if err := writeOwnerState(d, state); err != nil {
				t.Fatal(err)
			}
			d.Close()
			assertLegacyReadUnchanged(t, p, fence == "blocked")
		})
	}
}

func assertLegacyReadUnchanged(t *testing.T, p paths, blocked bool) {
	t.Helper()
	receiptFile := filepath.Join(p.applicationData, maintenanceReceiptName("legacy-request"))
	stateFile := filepath.Join(p.applicationData, "state.json")
	before, _ := os.ReadFile(receiptFile)
	stateBefore, _ := os.ReadFile(stateFile)
	var output bytes.Buffer
	if err := writeOperationReport(p, []string{"--request-id", "legacy-request", "--registration-id", "legacy-registration"}, &output); err == nil {
		t.Fatal("unproven read succeeded")
	}
	if blocked && !bytes.Contains(output.Bytes(), []byte(`"code":"request_blocked"`)) {
		t.Fatalf("blocked proof lost: %s", output.String())
	}
	after, _ := os.ReadFile(receiptFile)
	stateAfter, _ := os.ReadFile(stateFile)
	if !bytes.Equal(before, after) || !bytes.Equal(stateBefore, stateAfter) {
		t.Fatal("failed read changed owner facts")
	}
}
