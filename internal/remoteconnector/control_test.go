package remoteconnector

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"testing"

	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"google.golang.org/grpc"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCapacityDimensionIsForwardedFromHostAdmissionAuthority(t *testing.T) {
	for dimension, want := range map[string]remotev1.CapacityDimension{
		"pids":             remotev1.CapacityDimension_CAPACITY_DIMENSION_PIDS,
		"writable":         remotev1.CapacityDimension_CAPACITY_DIMENSION_WRITABLE,
		"storage_headroom": remotev1.CapacityDimension_CAPACITY_DIMENSION_STORAGE_HEADROOM,
		"import_slot":      remotev1.CapacityDimension_CAPACITY_DIMENSION_IMPORT_SLOT,
	} {
		value := status.New(codes.ResourceExhausted, "capacity exhausted")
		value, err := value.WithDetails(&errdetails.ErrorInfo{Domain: hostv1.ErrorDomain, Reason: "RESOURCE_CAPACITY_EXHAUSTED", Metadata: map[string]string{"resource_dimension": dimension}})
		if err != nil {
			t.Fatal(err)
		}
		if got := capacityDimensionFor(value.Err()); got != want {
			t.Fatalf("%s dimension = %s, want %s", dimension, got, want)
		}
	}
	if got := capacityDimensionFor(status.Error(codes.ResourceExhausted, "capacity exhausted")); got != remotev1.CapacityDimension_CAPACITY_DIMENSION_UNSPECIFIED {
		t.Fatalf("unknown dimension = %s", got)
	}
}

type ensureErrorClient struct {
	connectorv1.RemoteComputeServiceClient
	err error
}

func (client ensureErrorClient) EnsureAllocation(context.Context, *connectorv1.EnsureAllocationRequest, ...grpc.CallOption) (*connectorv1.AllocationResponse, error) {
	return nil, client.err
}

func TestHostAdmissionErrorsPreserveDispatchOutcome(t *testing.T) {
	for _, test := range []struct {
		code   codes.Code
		reason hostv1.ErrorReason
		remote remotev1.ErrorReason
	}{
		{codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED, remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED},
		{codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_REGISTRATION_DRAINING, remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED},
		{codes.Aborted, hostv1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED, remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED},
		{codes.Aborted, hostv1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH, remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH},
		{codes.Aborted, hostv1.ErrorReason_ERROR_REASON_ALLOCATION_CONFLICT, remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{codes.PermissionDenied, hostv1.ErrorReason_ERROR_REASON_POOL_EGRESS_LIMIT, remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{codes.ResourceExhausted, hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED, remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED},
		{codes.InvalidArgument, 0, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT},
		{codes.OutOfRange, 0, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT},
		{codes.NotFound, 0, remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND},
		{codes.FailedPrecondition, 0, remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{codes.Aborted, 0, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{codes.AlreadyExists, 0, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{codes.PermissionDenied, 0, remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED},
		{codes.Unauthenticated, 0, remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED},
		{codes.ResourceExhausted, 0, remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED},
	} {
		t.Run(test.code.String()+"/"+test.reason.String(), func(t *testing.T) {
			err := hostv1.NewError(test.code, test.reason, "host rejection")
			result := (commandHandler{logger: slog.Default(), client: ensureErrorClient{err: err}}).handle(t.Context(), &remotev1.ControllerCommand{CommandId: "ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "a", Generation: 1}}})
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED || result.GetReason() != test.remote {
				t.Fatalf("result = %v", result)
			}
		})
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown, codes.Internal, codes.DataLoss} {
		for _, reason := range []hostv1.ErrorReason{0, hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED, hostv1.ErrorReason_ERROR_REASON_VM_ADMISSION_CLOSED} {
			err := hostv1.NewError(code, reason, "unknown result")
			_, rejected := hostRejection(err)
			if rejected {
				t.Fatalf("postcommit code %v reason %v reported rejection", code, reason)
			}
		}
	}
}

func TestOpenSessionLocalRefusalsAreRejectedBeforeHostDispatch(t *testing.T) {
	valid := &remotev1.OpenSession{AllocationId: "allocation", AllocationGeneration: 1, ExecutionOwnerId: "owner"}
	atLimit := newTunnelManager(context.Background(), nil, nil, "registration", "connection", 1, nil)
	for index := 0; index < MaxActiveTunnels; index++ {
		atLimit.active[strconv.Itoa(index)] = &activeTunnel{}
	}
	for _, test := range []struct {
		manager *tunnelManager
		request *remotev1.OpenSession
		reason  remotev1.ErrorReason
	}{
		{nil, valid, remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE},
		{atLimit, &remotev1.OpenSession{}, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT},
		{atLimit, valid, remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED},
	} {
		result := (commandHandler{logger: slog.Default(), tunnels: test.manager}).handle(context.Background(), &remotev1.ControllerCommand{CommandId: "open", Command: &remotev1.ControllerCommand_OpenSession{OpenSession: test.request}})
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED || result.GetReason() != test.reason {
			t.Fatalf("result = %+v", result)
		}
	}
}

func TestReleaseAllocationRequiresExactRevisionAndGenerationFence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		target     uint64
		expected   uint64
		generation uint64
	}{
		{name: "missing", target: 0, expected: 0},
		{name: "mismatch", target: 7, expected: 6, generation: 2},
		{name: "missing generation", target: 7, expected: 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := &remotev1.ControllerCommand{
				CommandId:       "release",
				Sequence:        1,
				ConnectionEpoch: 9,
				TimeoutMillis:   1000,
				TargetRevision:  test.target,
				Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{
					AllocationId: "allocation", ExpectedRevision: test.expected, ExpectedGeneration: test.generation,
				}},
			}
			result, _ := newCommandTracker(9, 1).admit(command)
			if result.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || result.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
				t.Fatalf("release result = %+v", result)
			}
		})
	}
}

func TestCommandTrackerFencesEpochSequenceDeadlineAndGeneration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		command *remotev1.ControllerCommand
		reason  remotev1.ErrorReason
	}{
		{name: "stale epoch", command: testCommand(1, 8), reason: remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH},
		{name: "sequence gap", command: testCommand(2, 9), reason: remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT},
		{name: "zero timeout", command: func() *remotev1.ControllerCommand { c := testCommand(1, 9); c.TimeoutMillis = 0; return c }(), reason: remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT},
		{name: "cleanup plan revision", command: func() *remotev1.ControllerCommand {
			command := testCommand(1, 9)
			command.TargetRevision = 8
			command.Command = &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "a", Kind: remotev1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: 9}}
			return command
		}(), reason: remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newCommandTracker(9, 4)
			ack, result := tracker.admit(test.command)
			if result != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || ack.GetReason() != test.reason {
				t.Fatalf("ack/result = %+v/%+v", ack, result)
			}
		})
	}
}

func TestCommandTrackerReplaysOnlyCompletedDuplicate(t *testing.T) {
	t.Parallel()
	tracker := newCommandTracker(9, 4)
	command := testCommand(1, 9)
	ack, cached := tracker.admit(command)
	if ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED || cached != nil {
		t.Fatalf("first admission = %+v/%+v", ack, cached)
	}
	result := &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: 1, ConnectionEpoch: 9, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 4}
	if err := tracker.record(command, result); err != nil {
		t.Fatal(err)
	}

	ack, cached = tracker.admit(command)
	if ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_DUPLICATE || cached.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		t.Fatalf("duplicate admission = %+v/%+v", ack, cached)
	}
	conflict := testCommand(1, 9)
	conflict.CommandId = "different"
	ack, cached = tracker.admit(conflict)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("conflicting duplicate = %+v/%+v", ack, cached)
	}
}

func TestCommandTrackerTerminalRejectionsConsumeExpectedSequence(t *testing.T) {
	t.Parallel()
	tracker := newCommandTracker(9, 4)

	stale := testCommand(1, 8)
	ack, cached := tracker.admit(stale)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH {
		t.Fatalf("stale epoch = %+v/%+v", ack, cached)
	}

	gap := testCommand(2, 9)
	gap.CommandId = "command-2"
	ack, cached = tracker.admit(gap)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("initial gap = %+v/%+v", ack, cached)
	}

	rejected := testCommand(1, 9)
	rejected.TimeoutMillis = 0
	ack, cached = tracker.admit(rejected)
	if cached != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT {
		t.Fatalf("rejection = %+v/%+v", ack, cached)
	}

	generation := testCommand(2, 9)
	generation.CommandId = "command-2"
	generation.TargetRevision = 3
	generation.Command = &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: "a", ExpectedRevision: 3, ExpectedGeneration: 0}}
	ack, cached = tracker.admit(generation)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
		t.Fatalf("generation rejection = %+v/%+v", ack, cached)
	}

	revision := testCommand(3, 9)
	revision.CommandId = "command-3"
	revision.TargetRevision = 8
	revision.Command = &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "a", Kind: remotev1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: 9}}
	ack, cached = tracker.admit(revision)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
		t.Fatalf("revision rejection = %+v/%+v", ack, cached)
	}

	gap = testCommand(5, 9)
	gap.CommandId = "command-5"
	ack, cached = tracker.admit(gap)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("subsequent gap = %+v/%+v", ack, cached)
	}

	next := testCommand(4, 9)
	next.CommandId = "command-4"
	ack, cached = tracker.admit(next)
	if cached != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED {
		t.Fatalf("next admission = %+v/%+v", ack, cached)
	}
}

func TestCommandTrackerRejectsInventoryWatermarkRegression(t *testing.T) {
	t.Parallel()
	tracker := newCommandTracker(9, 5)
	command := testCommand(1, 9)
	if err := tracker.record(command, &remotev1.CommandResult{InventoryWatermark: 4}); err == nil {
		t.Fatal("inventory watermark regression accepted")
	}
}

func TestControllerReadyRequiresExactSnapshotFence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		ready *remotev1.ControllerReady
	}{
		{name: "missing"},
		{name: "unsupported feature", ready: &remotev1.ControllerReady{RegistrationId: "registration", ConnectionEpoch: 7, AcknowledgedInventoryWatermark: 9, EnabledFeatures: []string{"unknown"}}},
		{name: "registration", ready: &remotev1.ControllerReady{RegistrationId: "other", ConnectionEpoch: 7, AcknowledgedInventoryWatermark: 9}},
		{name: "epoch", ready: &remotev1.ControllerReady{RegistrationId: "registration", ConnectionEpoch: 6, AcknowledgedInventoryWatermark: 9}},
		{name: "watermark", ready: &remotev1.ControllerReady{RegistrationId: "registration", ConnectionEpoch: 7, AcknowledgedInventoryWatermark: 8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateControllerReady(test.ready, "registration", 7, 9); err == nil {
				t.Fatal("invalid ControllerReady was accepted")
			}
		})
	}
	if err := validateControllerReady(&remotev1.ControllerReady{RegistrationId: "registration", ConnectionEpoch: 7, AcknowledgedInventoryWatermark: 9}, "registration", 7, 9); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteObservationRequiresControllerReadyCapability(t *testing.T) {
	t.Parallel()
	if remoteObservationEnabled(&remotev1.ControllerReady{}) {
		t.Fatal("legacy ControllerReady enabled remote observations")
	}
	if !remoteObservationEnabled(&remotev1.ControllerReady{EnabledFeatures: []string{remoteObservationFeature}}) {
		t.Fatal("negotiated ControllerReady did not enable remote observations")
	}
}

func testCommand(sequence, epoch uint64) *remotev1.ControllerCommand {
	return &remotev1.ControllerCommand{CommandId: "command-1", Sequence: sequence, ConnectionEpoch: epoch, TimeoutMillis: 60000, Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
}

func TestCommandTrackerKeepsBoundedReplayWindow(t *testing.T) {
	t.Parallel()
	tracker := newCommandTracker(19, 7)
	for sequence := uint64(1); sequence <= 300; sequence++ {
		command := &remotev1.ControllerCommand{CommandId: fmt.Sprintf("command-%d", sequence), Sequence: sequence, ConnectionEpoch: 19, TimeoutMillis: 1000}
		if sequence%2 == 0 {
			command.TimeoutMillis = 0
		}
		ack, _ := tracker.admit(command)
		if sequence%2 == 1 {
			if ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED {
				t.Fatal(ack)
			}
			if err := tracker.record(command, &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: sequence, ConnectionEpoch: 19, InventoryWatermark: 7}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(tracker.slots) != 128 {
		t.Fatal("replay window changed")
	}
	for _, sequence := range []uint64{1, 299} {
		ack, result := tracker.admit(&remotev1.ControllerCommand{CommandId: fmt.Sprintf("command-%d", sequence), Sequence: sequence, ConnectionEpoch: 19})
		if sequence == 1 && (ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT || ack.GetMessage() != "command outcome is not replayable") {
			t.Fatal(ack)
		}
		if sequence == 299 && (ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_DUPLICATE || result.GetSequence() != sequence) {
			t.Fatal(ack, result)
		}
	}
}
