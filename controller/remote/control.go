package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

var ErrUnknownOutcome = errors.New("remote command outcome is unknown")

var RequiredFeatures = []string{
	"command-evidence-v1",
	"connection-epoch-v1",
	"inventory-watermark-v1",
	"unknown-outcome-v1",
}

type ControlStream interface {
	Send(*remotev1.ControllerControlMessage) error
	Recv() (*remotev1.HostControlMessage, error)
	Context() context.Context
}

type ControlSession struct {
	stream   ControlStream
	hello    *remotev1.HostHello
	sequence uint64
}

// ControlSession is the controller-side implementation anchor for
// Cue/tla/salix/AgentVMMControl.tla. It intentionally exposes unknown outcomes
// instead of replaying an admitted side-effecting command.

type CommandTranscript struct {
	Ack      *remotev1.CommandAck
	Evidence []*remotev1.CommandEvidence
	Result   *remotev1.CommandResult
}

func AcceptControl(stream ControlStream, registrationID string) (*ControlSession, error) {
	message, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	hello := message.GetHello()
	if err := ValidateHello(hello, registrationID); err != nil {
		return nil, err
	}
	return &ControlSession{stream: stream, hello: hello}, nil
}

func (session *ControlSession) Hello() *remotev1.HostHello { return session.hello }

func (session *ControlSession) Dispatch(ctx context.Context, command *remotev1.ControllerCommand) (CommandTranscript, error) {
	if command == nil || command.GetCommandId() == "" {
		return CommandTranscript{}, errors.New("command ID is required")
	}
	session.sequence++
	command.Sequence = session.sequence
	command.ConnectionEpoch = session.hello.GetConnectionEpoch()
	if deadline, ok := ctx.Deadline(); ok {
		command.DeadlineUnixMillis = deadline.UnixMilli()
	}
	if command.GetDeadlineUnixMillis() <= time.Now().UnixMilli() {
		return CommandTranscript{}, errors.New("command deadline is required")
	}
	if err := session.stream.Send(&remotev1.ControllerControlMessage{Payload: &remotev1.ControllerControlMessage_Command{Command: command}}); err != nil {
		return CommandTranscript{}, err
	}

	transcript := CommandTranscript{}
	for {
		message, err := session.stream.Recv()
		if err != nil {
			if transcript.Ack != nil && transcript.Ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED {
				transcript.Result = unknownResult(command, session.hello.GetInventoryWatermark())
				return transcript, fmt.Errorf("%w: %v", ErrUnknownOutcome, err)
			}
			return transcript, err
		}
		switch payload := message.GetPayload().(type) {
		case *remotev1.HostControlMessage_Ack:
			if err := validateIdentity(command, payload.Ack.GetCommandId(), payload.Ack.GetSequence(), payload.Ack.GetConnectionEpoch()); err != nil {
				return transcript, err
			}
			transcript.Ack = payload.Ack
			if payload.Ack.GetDisposition() == remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED {
				transcript.Result = &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED, Reason: payload.Ack.GetReason(), Message: payload.Ack.GetMessage(), InventoryWatermark: session.hello.GetInventoryWatermark()}
				return transcript, nil
			}
		case *remotev1.HostControlMessage_Evidence:
			if transcript.Ack == nil {
				return transcript, errors.New("execution evidence arrived before admission ACK")
			}
			if err := validateIdentity(command, payload.Evidence.GetCommandId(), payload.Evidence.GetSequence(), payload.Evidence.GetConnectionEpoch()); err != nil {
				return transcript, err
			}
			transcript.Evidence = append(transcript.Evidence, payload.Evidence)
		case *remotev1.HostControlMessage_Result:
			if transcript.Ack == nil {
				return transcript, errors.New("result arrived before admission ACK")
			}
			if err := validateIdentity(command, payload.Result.GetCommandId(), payload.Result.GetSequence(), payload.Result.GetConnectionEpoch()); err != nil {
				return transcript, err
			}
			transcript.Result = payload.Result
			if payload.Result.GetInventoryWatermark() > session.hello.GetInventoryWatermark() {
				session.hello.InventoryWatermark = payload.Result.GetInventoryWatermark()
			}
			return transcript, nil
		case *remotev1.HostControlMessage_Event:
			continue
		default:
			return transcript, errors.New("unexpected host control message")
		}
	}
}

func ValidateHello(hello *remotev1.HostHello, registrationID string) error {
	if hello == nil || hello.GetRegistrationId() == "" || hello.GetRegistrationId() != registrationID {
		return errors.New("invalid remote registration identity")
	}
	if hello.GetConnectionEpoch() == 0 || hello.GetProtocolVersion() == "" || hello.GetHostApiVersion() == "" {
		return errors.New("remote connection contract is incomplete")
	}
	if !containsAll(hello.GetSupportedFeatures(), RequiredFeatures) || !containsAll(hello.GetRequiredFeatures(), RequiredFeatures) {
		return errors.New("remote feature negotiation failed")
	}
	if len(hello.GetInventory()) > 128 {
		return errors.New("remote inventory exceeds limit")
	}
	seen := make(map[string]struct{}, len(hello.GetInventory()))
	var watermark uint64
	for _, allocation := range hello.GetInventory() {
		if allocation.GetAllocationId() == "" {
			return errors.New("remote inventory contains an empty allocation ID")
		}
		if _, exists := seen[allocation.GetAllocationId()]; exists {
			return errors.New("remote inventory contains a duplicate allocation")
		}
		seen[allocation.GetAllocationId()] = struct{}{}
		if allocation.GetRevision() > watermark {
			watermark = allocation.GetRevision()
		}
	}
	if hello.GetInventoryWatermark() < watermark {
		return errors.New("remote inventory watermark is stale")
	}
	return nil
}

func validateIdentity(command *remotev1.ControllerCommand, commandID string, sequence, epoch uint64) error {
	if commandID != command.GetCommandId() || sequence != command.GetSequence() || epoch != command.GetConnectionEpoch() {
		return errors.New("remote command transcript identity mismatch")
	}
	return nil
}

func unknownResult(command *remotev1.ControllerCommand, watermark uint64) *remotev1.CommandResult {
	return &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, Reason: remotev1.ErrorReason_ERROR_REASON_UNAVAILABLE, Message: "result lost after admission; read authoritative inventory", InventoryWatermark: watermark}
}

func containsAll(values, required []string) bool {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	for _, value := range required {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func IsUnknownOutcome(err error) bool {
	return errors.Is(err, ErrUnknownOutcome) || errors.Is(err, io.EOF)
}
