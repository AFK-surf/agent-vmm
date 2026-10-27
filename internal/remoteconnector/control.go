package remoteconnector

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"math"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/protobuf/proto"
)

const remoteObservationFeature = controllerremote.RemoteObservationFeature

var protocolFeatures = []string{remoteObservationFeature}

type commandTracker struct {
	epoch              uint64
	nextSequence       uint64
	inventoryWatermark uint64
	slots              [128]struct {
		seq    uint64
		id     string
		result *remotev1.CommandResult
	}
}

func newCommandTracker(epoch, inventoryWatermark uint64) *commandTracker {
	return &commandTracker{
		epoch: epoch, nextSequence: 1, inventoryWatermark: inventoryWatermark,
	}
}

func (tracker *commandTracker) admit(command *remotev1.ControllerCommand) (*remotev1.CommandAck, *remotev1.CommandResult) {
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
		slot := &tracker.slots[command.GetSequence()%uint64(len(tracker.slots))]
		if slot.seq != command.GetSequence() {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT, "command outcome is not replayable"), nil
		}
		if slot.id != command.GetCommandId() {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_SEQUENCE_CONFLICT, "sequence belongs to another command"), nil
		}
		cached := slot.result
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
	slot := &tracker.slots[command.GetSequence()%uint64(len(tracker.slots))]
	slot.seq, slot.id, slot.result = command.GetSequence(), command.GetCommandId(), nil
	tracker.nextSequence++
	if command.GetTimeoutMillis() == 0 || command.GetTimeoutMillis() > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "command timeout is invalid"), nil
	}
	if ensure := command.GetEnsureAllocation(); ensure != nil {
		if ensure.GetGeneration() == 0 || ensure.GetLimits() == nil {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, "allocation generation and limits are required"), nil
		}
		if command.GetTargetRevision() != ensure.GetPolicyRevision() {
			return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "target revision fence mismatch"), nil
		}
	}
	if release := command.GetReleaseAllocation(); release != nil && (release.GetExpectedRevision() == 0 || release.GetExpectedGeneration() == 0 || command.GetTargetRevision() != release.GetExpectedRevision()) {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "allocation release fence mismatch"), nil
	}
	if disposal := command.GetDisposeAllocation(); disposal != nil && (disposal.GetExpectedRevision() == 0 || disposal.GetExpectedGeneration() == 0 || command.GetTargetRevision() != disposal.GetExpectedRevision() || disposal.GetRequestId() == "") {
		return rejectAck(ack, remotev1.ErrorReason_ERROR_REASON_STALE_REVISION, "allocation deletion fence mismatch"), nil
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
	slot := &tracker.slots[command.GetSequence()%uint64(len(tracker.slots))]
	if slot.seq == command.GetSequence() {
		slot.result = proto.Clone(result).(*remotev1.CommandResult)
	}
	return nil
}

func rejectAck(ack *remotev1.CommandAck, reason remotev1.ErrorReason, message string) *remotev1.CommandAck {
	ack.Disposition = remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED
	ack.Reason = reason
	ack.Message = message
	return ack
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
	if !containsAllFeatures(protocolFeatures, ready.GetEnabledFeatures()) {
		return &localRejection{reason: remotev1.ErrorReason_ERROR_REASON_FEATURE_MISMATCH, message: "controller enabled unsupported features"}
	}
	return nil
}

func remoteObservationEnabled(ready *remotev1.ControllerReady) bool {
	return ready != nil && containsAllFeatures(ready.GetEnabledFeatures(), []string{remoteObservationFeature})
}
