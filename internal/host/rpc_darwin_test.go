//go:build darwin

package host

import (
	"errors"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/codes"
)

func TestRPCRateLimiterEnforcesBurst(t *testing.T) {
	limiter := newRPCRateLimiter(1, 2)
	if !limiter.allow() || !limiter.allow() {
		t.Fatal("initial burst was rejected")
	}
	if limiter.allow() {
		t.Fatal("request beyond the burst was accepted")
	}
}

func TestAuditOutcomeUsesOperationFailure(t *testing.T) {
	failed := &hostv1.OperationResponse{Operation: &hostv1.Operation{
		State: hostv1.OperationState_OPERATION_STATE_FAILED,
		Error: &hostv1.OperationError{Reason: "DIAGNOSTIC_FAILED"},
	}}
	if code, reason := auditOutcome(failed, nil); code != codes.Internal || reason != "DIAGNOSTIC_FAILED" {
		t.Fatalf("failed operation audit = %v/%q", code, reason)
	}
	if code, reason := auditOutcome(nil, errors.New("failure")); code != codes.Unknown || reason != "RPC_UNKNOWN" {
		t.Fatalf("RPC failure audit = %v/%q", code, reason)
	}
	if code, reason := auditOutcome(&hostv1.OperationResponse{Operation: &hostv1.Operation{State: hostv1.OperationState_OPERATION_STATE_SUCCEEDED}}, nil); code != codes.OK || reason != "" {
		t.Fatalf("successful operation audit = %v/%q", code, reason)
	}
}

func TestProtoStringFieldTraversesRepeatedMessages(t *testing.T) {
	request := &hostv1.ExecuteCleanupRequest{
		RequestId: "request-a",
		Plan: &hostv1.CleanupPlan{
			EnvironmentId: "environment-a",
			Impacts:       []*hostv1.CleanupImpact{{Resource: &hostv1.ResourceReference{EnvironmentId: "nested-environment"}}},
		},
		ConfirmedVolumeIds: []string{"volume-a"},
	}
	if got := protoStringField(request.ProtoReflect(), "request_id"); got != "request-a" {
		t.Fatalf("request ID = %q", got)
	}
	if got := protoStringField(request.ProtoReflect(), "environment_id"); got != "environment-a" {
		t.Fatalf("environment ID = %q", got)
	}
}
