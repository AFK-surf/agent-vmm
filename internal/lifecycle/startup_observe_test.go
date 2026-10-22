//go:build darwin || linux

package lifecycle

import "testing"

func TestStartupEvidenceTracksSuccessAndConfirmedFailure(t *testing.T) {
	operation := &lifecycleOperation{OperationID: "host-operation", RequestID: "request-9"}
	result := &startupEvidence{}
	applyStartupRecords(result, []byte(
		`vmm_start={"operation_id":"vm-start-operation","request_id":"request-9","stage":"verify_bundle","state":"succeeded"}`+"\n"+
			`vmm_start={"operation_id":"vm-start-operation","request_id":"request-9","stage":"start_machine","state":"failed","error":"hypervisor rejected configuration"}`+"\n"), "vmm_start=", operation)
	if result.OperationID != "vm-start-operation" || result.RequestID != operation.RequestID || result.LastSuccessfulStage != "verify_bundle" ||
		result.FailureStage != "start_machine" || result.FailureMessage != "hypervisor rejected configuration" || !result.FailureConfirmed {
		t.Fatalf("startup evidence = %+v", result)
	}
}

func TestStartupEvidenceRejectsDifferentRequest(t *testing.T) {
	result := &startupEvidence{}
	applyStartupRecords(result, []byte(`vmm_start={"operation_id":"other","request_id":"other","stage":"start_machine","state":"failed","error":"wrong attempt"}`+"\n"),
		"vmm_start=", &lifecycleOperation{OperationID: "current", RequestID: "current"})
	if result.CurrentStage != "" || result.FailureConfirmed {
		t.Fatalf("unrelated startup evidence was accepted: %+v", result)
	}
}

func TestGuestProgressDoesNotOverwriteCompletedHostStart(t *testing.T) {
	result := &startupEvidence{}
	operation := &lifecycleOperation{RequestID: "request-9"}
	applyStartupRecords(result, []byte(`vmm_start={"operation_id":"vm-start","request_id":"request-9","stage":"wait_guest_health","state":"succeeded"}`+"\n"), "vmm_start=", operation)
	if applied := applyStartupRecords(result, []byte(`guest_startup={"operation_id":"vm-start","stage":"listener","state":"succeeded"}`+"\n"), "guest_startup=", operation); applied {
		t.Fatal("older Guest progress overwrote completed Host start")
	}
	if result.CurrentStage != "wait_guest_health" || result.CurrentState != "succeeded" {
		t.Fatalf("startup evidence = %+v", result)
	}
}

func TestOrphanGuestProgressIsNotAssignedToLifecycleRequest(t *testing.T) {
	result := &startupEvidence{RequestID: "current-request"}
	applied := applyStartupRecords(result, []byte(`guest_startup={"operation_id":"old-vm-start","stage":"listener","state":"failed","error":"old failure"}`+"\n"),
		"guest_startup=", &lifecycleOperation{RequestID: "current-request"})
	if applied || result.OperationID != "" || result.FailureConfirmed {
		t.Fatalf("orphan Guest evidence was assigned to the current request: %+v", result)
	}
}
