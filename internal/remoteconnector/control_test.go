package remoteconnector

import (
	"context"
	"google.golang.org/grpc"
	"strconv"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCapacityDimensionIsForwardedFromHostAdmissionAuthority(t *testing.T) {
	for dimension, want := range map[string]remotev1.CapacityDimension{
		"cpu_max":          remotev1.CapacityDimension_CAPACITY_DIMENSION_CPU_MAX,
		"memory":           remotev1.CapacityDimension_CAPACITY_DIMENSION_MEMORY,
		"pids":             remotev1.CapacityDimension_CAPACITY_DIMENSION_PIDS,
		"writable":         remotev1.CapacityDimension_CAPACITY_DIMENSION_WRITABLE,
		"storage_headroom": remotev1.CapacityDimension_CAPACITY_DIMENSION_STORAGE_HEADROOM,
		"import_slot":      remotev1.CapacityDimension_CAPACITY_DIMENSION_IMPORT_SLOT,
	} {
		value := status.New(codes.ResourceExhausted, "capacity exhausted")
		value, err := value.WithDetails(&errdetails.ErrorInfo{Reason: "RESOURCE_LEDGER_EXHAUSTED", Metadata: map[string]string{"resource_dimension": dimension}})
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
	hostv1.RemoteComputeServiceClient
	err error
}

func (client ensureErrorClient) EnsureAllocation(context.Context, *hostv1.EnsureAllocationRequest, ...grpc.CallOption) (*hostv1.AllocationResponse, error) {
	return nil, client.err
}

func TestHostAdmissionErrorsPreserveDispatchOutcome(t *testing.T) {
	for _, test := range []struct {
		reason string
		remote remotev1.ErrorReason
	}{
		{"VM_ADMISSION_CLOSED", remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED},
		{"REGISTRATION_DRAINING", remotev1.ErrorReason_ERROR_REASON_ADMISSION_PAUSED},
		{"STALE_CONNECTION", remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{"POLICY_REVISION_CONFLICT", remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{"REGISTRATION_NOT_ENABLED", remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{"ALLOCATION_CONFLICT", remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{"POOL_EGRESS_LIMIT", remotev1.ErrorReason_ERROR_REASON_POLICY_CONFLICT},
		{"RESOURCE_LEDGER_EXHAUSTED", remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED},
		{"POOL_PER_ENVIRONMENT_LIMIT", remotev1.ErrorReason_ERROR_REASON_CAPACITY_EXHAUSTED},
		{"INVALID_ARGUMENT", remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT},
		{"REGISTRATION_MISMATCH", remotev1.ErrorReason_ERROR_REASON_UNAUTHENTICATED},
		{"ALLOCATION_NOT_FOUND", remotev1.ErrorReason_ERROR_REASON_ALLOCATION_NOT_FOUND},
		{"DISPOSAL_REVISION_NOT_ACCEPTED", remotev1.ErrorReason_ERROR_REASON_DISPOSAL_REVISION_NOT_ACCEPTED},
	} {
		t.Run(test.reason, func(t *testing.T) {
			value, err := status.New(codes.Unavailable, "host rejection").WithDetails(&errdetails.ErrorInfo{Reason: test.reason})
			if err != nil {
				t.Fatal(err)
			}
			result := (commandHandler{client: ensureErrorClient{err: value.Err()}}).handle(context.Background(), &remotev1.ControllerCommand{CommandId: "ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "a", Generation: 1}}})
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED || result.GetReason() != test.remote {
				t.Fatalf("result = %+v", result)
			}
		})
	}
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, status.Error(codes.DeadlineExceeded, "deadline"), status.Error(codes.ResourceExhausted, "no detail"), status.Error(codes.PermissionDenied, "no detail"), func() error {
		value, _ := status.New(codes.Unavailable, "unrecognized").WithDetails(&errdetails.ErrorInfo{Reason: "UNRECOGNIZED_HOST_REASON"})
		return value.Err()
	}()} {
		result := (commandHandler{client: ensureErrorClient{err: failure}}).handle(context.Background(), &remotev1.ControllerCommand{CommandId: "ensure", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "a", Generation: 1}}})
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN || result.GetMessage() != failure.Error() {
			t.Fatalf("failure %v result = %+v", failure, result)
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
		result := (commandHandler{tunnels: test.manager}).handle(context.Background(), &remotev1.ControllerCommand{CommandId: "open", Command: &remotev1.ControllerCommand_OpenSession{OpenSession: test.request}})
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
				CommandId:      "release",
				TargetRevision: test.target,
				Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{
					AllocationId: "allocation", ExpectedRevision: test.expected, ExpectedGeneration: test.generation,
				}},
			}
			result := (commandHandler{}).handle(context.Background(), command)
			if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED || result.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
				t.Fatalf("release result = %+v", result)
			}
		})
	}
}

func TestCommandTrackerFencesEpochSequenceDeadlineAndGeneration(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	tests := []struct {
		name    string
		command *remotev1.ControllerCommand
		reason  remotev1.ErrorReason
	}{
		{name: "stale epoch", command: testCommand(1, 8, now.Add(time.Minute)), reason: remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH},
		{name: "sequence gap", command: testCommand(2, 9, now.Add(time.Minute)), reason: remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT},
		{name: "deadline", command: testCommand(1, 9, now), reason: remotev1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED},
		{name: "unexpected target generation", command: func() *remotev1.ControllerCommand {
			command := testCommand(1, 9, now.Add(time.Minute))
			command.TargetGeneration = 3
			command.Command = &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: "a", AllocationGeneration: 2, ExecutionOwnerId: "runtime:a:1"}}
			return command
		}(), reason: remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{name: "cleanup plan revision", command: func() *remotev1.ControllerCommand {
			command := testCommand(1, 9, now.Add(time.Minute))
			command.TargetRevision = 8
			command.Command = &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "a", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: 9}}
			return command
		}(), reason: remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
		{name: "frontend allocation revision", command: func() *remotev1.ControllerCommand {
			command := testCommand(1, 9, now.Add(time.Minute))
			command.Command = &remotev1.ControllerCommand_AcquireHostLoopbackFrontend{AcquireHostLoopbackFrontend: &remotev1.AcquireHostLoopbackFrontend{AllocationId: "a", AllocationGeneration: 2}}
			return command
		}(), reason: remotev1.ErrorReason_ERROR_REASON_STALE_REVISION},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newCommandTracker(9, 4)
			ack, result := tracker.admit(test.command, now)
			if result != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || ack.GetReason() != test.reason {
				t.Fatalf("ack/result = %+v/%+v", ack, result)
			}
		})
	}
}

func TestCommandTrackerReplaysOnlyCompletedDuplicate(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	tracker := newCommandTracker(9, 4)
	command := testCommand(1, 9, now.Add(time.Minute))
	ack, cached := tracker.admit(command, now)
	if ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED || cached != nil {
		t.Fatalf("first admission = %+v/%+v", ack, cached)
	}
	result := &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: 1, ConnectionEpoch: 9, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 4}
	if err := tracker.record(command, result); err != nil {
		t.Fatal(err)
	}

	ack, cached = tracker.admit(command, now)
	if ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_DUPLICATE || cached.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		t.Fatalf("duplicate admission = %+v/%+v", ack, cached)
	}
	conflict := testCommand(1, 9, now.Add(time.Minute))
	conflict.CommandId = "different"
	ack, cached = tracker.admit(conflict, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("conflicting duplicate = %+v/%+v", ack, cached)
	}
}

func TestCommandTrackerTerminalRejectionsConsumeExpectedSequence(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	tracker := newCommandTracker(9, 4)

	stale := testCommand(1, 8, now.Add(time.Minute))
	ack, cached := tracker.admit(stale, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH {
		t.Fatalf("stale epoch = %+v/%+v", ack, cached)
	}

	gap := testCommand(2, 9, now.Add(time.Minute))
	gap.CommandId = "command-2"
	ack, cached = tracker.admit(gap, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("initial gap = %+v/%+v", ack, cached)
	}

	rejected := testCommand(1, 9, now)
	ack, cached = tracker.admit(rejected, now)
	if cached != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED {
		t.Fatalf("rejection = %+v/%+v", ack, cached)
	}

	generation := testCommand(2, 9, now.Add(time.Minute))
	generation.CommandId = "command-2"
	generation.TargetGeneration = 3
	generation.Command = &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: "a", AllocationGeneration: 2, ExecutionOwnerId: "runtime:a:1"}}
	ack, cached = tracker.admit(generation, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
		t.Fatalf("generation rejection = %+v/%+v", ack, cached)
	}

	revision := testCommand(3, 9, now.Add(time.Minute))
	revision.CommandId = "command-3"
	revision.TargetRevision = 8
	revision.Command = &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "a", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: 9}}
	ack, cached = tracker.admit(revision, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_STALE_REVISION {
		t.Fatalf("revision rejection = %+v/%+v", ack, cached)
	}

	gap = testCommand(5, 9, now.Add(time.Minute))
	gap.CommandId = "command-5"
	ack, cached = tracker.admit(gap, now)
	if cached != nil || ack.GetReason() != remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT {
		t.Fatalf("subsequent gap = %+v/%+v", ack, cached)
	}

	next := testCommand(4, 9, now.Add(time.Minute))
	next.CommandId = "command-4"
	ack, cached = tracker.admit(next, now)
	if cached != nil || ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED {
		t.Fatalf("next admission = %+v/%+v", ack, cached)
	}
}

func TestCommandTrackerRejectsInventoryWatermarkRegression(t *testing.T) {
	t.Parallel()
	tracker := newCommandTracker(9, 5)
	command := testCommand(1, 9, time.Now().Add(time.Minute))
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

func testCommand(sequence, epoch uint64, deadline time.Time) *remotev1.ControllerCommand {
	return &remotev1.ControllerCommand{CommandId: "command-1", Sequence: sequence, ConnectionEpoch: epoch, DeadlineUnixMillis: deadline.UnixMilli(), Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
}
