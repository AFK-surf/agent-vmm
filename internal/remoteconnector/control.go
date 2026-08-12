package remoteconnector

import (
	"crypto/rand"
	"encoding/binary"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/protobuf/proto"
)

var protocolFeatures = []string{
	"command-evidence-v1",
	"connection-epoch-v1",
	"inventory-watermark-v1",
	"unknown-outcome-v1",
}

type commandTracker struct {
	epoch              uint64
	nextSequence       uint64
	inventoryWatermark uint64
	results            map[uint64]*remotev1.CommandResult
	commandIDs         map[uint64]string
}

// Agent VMM command delivery and unknown-outcome transitions are modeled in
// Cue/tla/salix/AgentVMMControl.tla. Keep epoch, sequence, admission, execution,
// result, and reconnect behavior aligned with that model.

func newCommandTracker(epoch, inventoryWatermark uint64) *commandTracker {
	return &commandTracker{
		epoch: epoch, nextSequence: 1, inventoryWatermark: inventoryWatermark,
		results: make(map[uint64]*remotev1.CommandResult), commandIDs: make(map[uint64]string),
	}
}

func (tracker *commandTracker) admit(command *remotev1.ControllerCommand, now time.Time) (*remotev1.CommandAck, *remotev1.CommandResult) {
	ack := &remotev1.CommandAck{
		CommandId: command.GetCommandId(), Sequence: command.GetSequence(),
		ConnectionEpoch: tracker.epoch,
	}
	if command.GetCommandId() == "" || len(command.GetCommandId()) > 128 {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "invalid command ID"), nil
	}
	if command.GetConnectionEpoch() != tracker.epoch {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_CONNECTION_EPOCH, "stale connection epoch"), nil
	}
	if command.GetSequence() < tracker.nextSequence {
		if tracker.commandIDs[command.GetSequence()] != command.GetCommandId() {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT, "sequence belongs to another command"), nil
		}
		cached := tracker.results[command.GetSequence()]
		if cached == nil {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT, "command outcome is not replayable"), nil
		}
		ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_DUPLICATE
		return ack, proto.Clone(cached).(*remotev1.CommandResult)
	}
	if command.GetSequence() != tracker.nextSequence {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT, "command sequence has a gap"), nil
	}
	if command.GetDeadlineUnixMillis() <= now.UnixMilli() {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "command deadline elapsed"), nil
	}
	if !validGenerationFence(command) {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "lease generation fence mismatch"), nil
	}
	if ensure := command.GetEnsureAllocation(); ensure != nil && command.GetTargetRevision() != ensure.GetPolicyRevision() {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "target revision fence mismatch"), nil
	}
	tracker.commandIDs[command.GetSequence()] = command.GetCommandId()
	tracker.nextSequence++
	ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED
	return ack, nil
}

func (tracker *commandTracker) record(command *remotev1.ControllerCommand, result *remotev1.CommandResult) {
	if allocation := result.GetAllocation(); allocation != nil && allocation.GetRevision() > tracker.inventoryWatermark {
		tracker.inventoryWatermark = allocation.GetRevision()
	}
	result.InventoryWatermark = tracker.inventoryWatermark
	tracker.results[command.GetSequence()] = proto.Clone(result).(*remotev1.CommandResult)
	if len(tracker.results) <= 128 {
		return
	}
	delete(tracker.results, command.GetSequence()-128)
	delete(tracker.commandIDs, command.GetSequence()-128)
}

func rejectAck(ack *remotev1.CommandAck, reason remotev1.ErrorReason, message string) *remotev1.CommandAck {
	ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED
	ack.Reason = reason
	ack.Message = message
	return ack
}

func validGenerationFence(command *remotev1.ControllerCommand) bool {
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_RenewLease:
		return command.GetLeaseGeneration() == value.RenewLease.GetGeneration()
	case *remotev1.ControllerCommand_ReleaseLease:
		return command.GetLeaseGeneration() == value.ReleaseLease.GetGeneration()
	case *remotev1.ControllerCommand_OpenSession:
		return command.GetLeaseGeneration() == value.OpenSession.GetGeneration()
	default:
		return command.GetLeaseGeneration() == 0
	}
}

func randomEpoch() (uint64, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, err
	}
	epoch := binary.BigEndian.Uint64(value[:])
	if epoch == 0 {
		epoch = 1
	}
	return epoch, nil
}

func inventoryWatermark(inventory []*remotev1.Allocation) uint64 {
	var watermark uint64
	for _, allocation := range inventory {
		if allocation.GetRevision() > watermark {
			watermark = allocation.GetRevision()
		}
	}
	return watermark
}
