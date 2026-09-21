package remoteconnector

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/protobuf/proto"
)

const remoteObservationFeature = "remote-observation-v1"

var runtimeCapabilityFeatures = []string{
	"oci-cross-namespace-v1",
	"readonly-root-tmpfs-v1",
}

var requiredProtocolFeatures = []string{
	"command-evidence-v1",
	"connection-epoch-v1",
	"controller-ready-v1",
	"endpoint-observation-renewal-v1",
	"inventory-watermark-v1",
	"host-loopback-frontend-v1",
	"unknown-outcome-v1",
	"resource-v2",
}

var protocolFeatures = append(append(append([]string(nil), requiredProtocolFeatures...), remoteObservationFeature), runtimeCapabilityFeatures...)

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
	// Sequence orders commands received on this epoch, including commands whose
	// deadline or operation fence makes them inadmissible. A rejection is a
	// terminal controller transcript and must not block the next issued command.
	tracker.commandIDs[command.GetSequence()] = command.GetCommandId()
	tracker.nextSequence++
	if command.GetDeadlineUnixMillis() <= now.UnixMilli() {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_DEADLINE_EXCEEDED, "command deadline elapsed"), nil
	}
	if !validGenerationFence(command) {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "target generation fence mismatch"), nil
	}
	if ensure := command.GetEnsureAllocation(); ensure != nil {
		if ensure.GetGeneration() == 0 || ensure.GetResourcesV2() == nil {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "resource-v2 allocation identity is required"), nil
		}
		if command.GetTargetRevision() != ensure.GetPolicyRevision() {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "target revision fence mismatch"), nil
		}
	}
	if release := command.GetReleaseAllocation(); release != nil && (release.GetExpectedGeneration() == 0 || command.GetTargetRevision() != release.GetExpectedRevision()) {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "allocation release fence mismatch"), nil
	}
	if command.GetAcquireHostLoopbackFrontend() != nil && command.GetTargetRevision() == 0 {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "frontend allocation revision fence is required"), nil
	}
	if cleanup := command.GetExecuteCleanupAllocation(); cleanup != nil && command.GetTargetRevision() != cleanup.GetPlanRevision() {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "cleanup plan revision fence mismatch"), nil
	}
	ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED
	return ack, nil
}

func (tracker *commandTracker) record(command *remotev1.ControllerCommand, result *remotev1.CommandResult) error {
	if result.GetInventoryWatermark() < tracker.inventoryWatermark {
		return errors.New("authoritative inventory watermark regressed")
	}
	tracker.inventoryWatermark = result.GetInventoryWatermark()
	tracker.results[command.GetSequence()] = proto.Clone(result).(*remotev1.CommandResult)
	if len(tracker.results) <= 128 {
		return nil
	}
	delete(tracker.results, command.GetSequence()-128)
	delete(tracker.commandIDs, command.GetSequence()-128)
	return nil
}

func rejectAck(ack *remotev1.CommandAck, reason remotev1.ErrorReason, message string) *remotev1.CommandAck {
	ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED
	ack.Reason = reason
	ack.Message = message
	return ack
}

func validGenerationFence(command *remotev1.ControllerCommand) bool {
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_RenewHostLoopbackFrontend:
		return command.GetTargetGeneration() == value.RenewHostLoopbackFrontend.GetGeneration()
	case *remotev1.ControllerCommand_ReleaseHostLoopbackFrontend:
		return command.GetTargetGeneration() == value.ReleaseHostLoopbackFrontend.GetGeneration()
	default:
		return command.GetTargetGeneration() == 0
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

func validateControllerReady(ready *remotev1.ControllerReady, registrationID string, epoch, watermark uint64) error {
	if ready == nil || ready.GetRegistrationId() != registrationID || ready.GetConnectionEpoch() != epoch || ready.GetAcknowledgedInventoryWatermark() != watermark {
		return errors.New("controller ready fence does not match the authoritative inventory snapshot")
	}
	return nil
}

func remoteObservationEnabled(ready *remotev1.ControllerReady) bool {
	return ready != nil && containsAllFeatures(ready.GetEnabledFeatures(), []string{remoteObservationFeature})
}
