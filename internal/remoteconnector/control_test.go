package remoteconnector

import (
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

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
		{name: "lease generation", command: func() *remotev1.ControllerCommand {
			command := testCommand(1, 9, now.Add(time.Minute))
			command.LeaseGeneration = 3
			command.Command = &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: "a", LeaseId: "l", Generation: 2}}
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
	result := &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: 1, ConnectionEpoch: 9, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED}
	tracker.record(command, result)

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

func testCommand(sequence, epoch uint64, deadline time.Time) *remotev1.ControllerCommand {
	return &remotev1.ControllerCommand{CommandId: "command-1", Sequence: sequence, ConnectionEpoch: epoch, DeadlineUnixMillis: deadline.UnixMilli(), Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
}
