//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorCatalogDetailsForLocalAndRemoteCallers(t *testing.T) {
	for name, number := range hostv1.ErrorReason_value {
		if number == 0 {
			continue
		}
		for _, registration := range []string{"", "registration"} {
			t.Run(name+"/"+registration, func(t *testing.T) {
				identity := auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "private-env", RegistrationID: registration}
				source := hostv1.NewError(codes.ResourceExhausted, hostv1.ErrorReason(number), "rejected", hostv1.WithResource("environment", "private-env"), hostv1.WithMetadata(map[string]string{"stage": "test"}))
				_, err := auth.UnaryInterceptor(identity)(t.Context(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) { return nil, addErrorDetails(ctx, source) })
				reason, ok := hostv1.ErrorReasonOf(err)
				if !ok || reason != hostv1.ErrorReason(number) {
					t.Fatalf("reason = %v, %t", reason, ok)
				}
				retry, resource := false, false
				for _, detail := range status.Convert(err).Details() {
					switch detail.(type) {
					case *errdetails.RetryInfo:
						retry = true
					case *errdetails.ResourceInfo:
						resource = true
					case *errdetails.ErrorInfo:
					default:
						t.Fatalf("unexpected detail %T", detail)
					}
				}
				if !retry || resource != (registration == "") {
					t.Fatalf("retry=%t resource=%t", retry, resource)
				}
			})
		}
	}
}

func TestRPCRateLimiterEnforcesBurst(t *testing.T) {
	limiter := newRPCRateLimiter(1, 2)
	if !limiter.allow() || !limiter.allow() {
		t.Fatal("initial burst was rejected")
	}
	if limiter.allow() {
		t.Fatal("request beyond the burst was accepted")
	}
}

func TestAuditOutcomeUsesRPCResult(t *testing.T) {
	if code, reason := auditOutcome(errors.New("failure")); code != codes.Unknown || reason != "" {
		t.Fatalf("RPC failure audit = %v/%q", code, reason)
	}
	if code, reason := auditOutcome(nil); code != codes.OK || reason != "" {
		t.Fatalf("RPC success audit = %v/%q", code, reason)
	}
	failure := hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "conflict")
	if code, reason := auditOutcome(failure); code != codes.Aborted || reason != "REQUEST_ID_CONFLICT" {
		t.Fatalf("catalog failure audit = %v/%q", code, reason)
	}
}

func TestAuditReceiptsExcludeHighFrequencyReadAndObservationRPCs(t *testing.T) {
	for _, method := range []string{
		connectorv1.RemoteComputeService_GetRemoteState_FullMethodName,
		connectorv1.RemoteComputeService_GetRemoteObservation_FullMethodName,
		hostv1.OperatorService_WatchMetrics_FullMethodName,
		vmmdv1.VMMDService_Health_FullMethodName,
		vmmdv1.VMMDService_ComponentHealth_FullMethodName,
		vmmdv1.VMMDService_InspectContainer_FullMethodName,
		vmmdv1.VMMDService_ProbeEgress_FullMethodName,
	} {
		if !rpcPolicies[method].readOnly {
			t.Fatalf("high-frequency observation would create a durable receipt: %s", method)
		}
	}
	for _, method := range []string{
		connectorv1.RemoteComputeService_EnsureAllocation_FullMethodName,
		connectorv1.RemoteComputeService_ReleaseAllocation_FullMethodName,
		connectorv1.RemoteComputeService_SetRemoteAdmissionState_FullMethodName,
		connectorv1.RemoteComputeService_ReportConnectorStatus_FullMethodName,
		hostv1.OperatorService_RunDiagnostics_FullMethodName,
		hostv1.OperatorService_ExportSupportBundle_FullMethodName,
		"/agentvmm.host.v1.OperatorService/FutureLifecycleMutation",
	} {
		if rpcPolicies[method].readOnly {
			t.Fatalf("lifecycle mutation lost its durable receipt: %s", method)
		}
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
