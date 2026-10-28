package remote

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	ErrUnknownOutcome = errors.New("remote command outcome is unknown")
	ErrSessionClosed  = errors.New("remote controller session is closed")
)

const (
	ProtocolVersion          = 4
	MinProtocolVersion       = 4
	RemoteObservationFeature = "remote-observation-v1"
	MaxActiveInventoryItems  = 64
)

type ControlStream interface {
	Send(*remotev1.ConnectResponse) error
	Recv() (*remotev1.ConnectRequest, error)
	Context() context.Context
}

type ControlSession struct {
	stream             ControlStream
	hello              *remotev1.HostHello
	lastSequence       uint64
	ready              bool
	dispatchMu         sync.Mutex
	messages           chan *remotev1.ConnectRequest
	observations       chan *remotev1.RemoteObservationRenewal
	done               chan struct{}
	stateMu            sync.Mutex
	terminateOnce      sync.Once
	err                error
	watermark          uint64
	observationEnabled bool
}

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
	return &ControlSession{stream: stream, hello: hello, watermark: hello.GetInventoryWatermark(), messages: make(chan *remotev1.ConnectRequest, 8), observations: make(chan *remotev1.RemoteObservationRenewal, 1), done: make(chan struct{})}, nil
}

func (session *ControlSession) Hello() *remotev1.HostHello {
	return proto.Clone(session.hello).(*remotev1.HostHello)
}

func (session *ControlSession) Observations() <-chan *remotev1.RemoteObservationRenewal {
	return session.observations
}
func (session *ControlSession) Done() <-chan struct{} { return session.done }
func (session *ControlSession) Err() error {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	return session.err
}

// Ready settles the exact authoritative inventory snapshot after the
// controller has durably reconciled it. Commands are forbidden before this
// explicit fence.
func (session *ControlSession) Ready(enabled ...string) error {
	session.dispatchMu.Lock()
	defer session.dispatchMu.Unlock()
	if session.ready {
		return errors.New("controller session is already ready")
	}
	session.observationEnabled = containsAll(enabled, []string{RemoteObservationFeature})
	ready := &remotev1.ControllerReady{RegistrationId: session.hello.GetRegistrationId(), ConnectionEpoch: session.hello.GetConnectionEpoch(), AcknowledgedInventoryWatermark: session.hello.GetInventoryWatermark(), EnabledFeatures: append([]string(nil), enabled...)}
	if err := session.stream.Send(&remotev1.ConnectResponse{Payload: &remotev1.ConnectResponse_ControllerReady{ControllerReady: ready}}); err != nil {
		return err
	}
	session.ready = true
	go session.receive()
	return nil
}

func (session *ControlSession) receive() {
	defer session.terminate(errors.New("host control receive loop ended"))
	for {
		message, err := session.stream.Recv()
		if err != nil {
			session.terminate(err)
			return
		}

		if renewal := message.GetObservationRenewal(); renewal != nil {
			if !session.observationEnabled {
				session.terminate(errors.New("remote observation feature was not negotiated"))
				return
			}
			if err := validateObservationRenewal(renewal, session.hello.GetConnectionEpoch()); err != nil {
				session.terminate(err)
				return
			}
			session.stateMu.Lock()
			if session.err != nil {
				session.stateMu.Unlock()
				return
			}
			select {
			case <-session.observations:
			default:
			}
			session.observations <- proto.Clone(renewal).(*remotev1.RemoteObservationRenewal)
			session.stateMu.Unlock()
			continue
		}
		switch payload := message.GetPayload().(type) {
		case *remotev1.ConnectRequest_EndpointObservationRenewal:
			if payload.EndpointObservationRenewal.GetConnectionEpoch() != session.hello.GetConnectionEpoch() || payload.EndpointObservationRenewal.GetEndpointObservation() == nil {
				session.terminate(errors.New("invalid endpoint observation renewal"))
				return
			}
			continue
		case *remotev1.ConnectRequest_Ack, *remotev1.ConnectRequest_Evidence, *remotev1.ConnectRequest_Result:
		default:
			session.terminate(errors.New("unexpected host control message"))
			return
		}
		select {
		case session.messages <- message:
		default:
			session.terminate(errors.New("host control message queue exhausted"))
			return
		}
	}
}

func validateObservationRenewal(value *remotev1.RemoteObservationRenewal, epoch uint64) error {
	observation := value.GetObservation()
	if value.GetConnectionEpoch() != epoch || observation == nil || observation.GetSequence() < 2 || observation.GetObservedUnixMillis() <= 0 || observation.GetInventoryObservedUnixMillis() <= 0 {
		return errors.New("invalid remote observation renewal")
	}
	return nil
}

func (session *ControlSession) terminate(err error) {
	session.terminateOnce.Do(func() {
		session.stateMu.Lock()
		defer session.stateMu.Unlock()
		session.err = err
		close(session.done)
		close(session.observations)
	})
}

func (session *ControlSession) Dispatch(ctx context.Context, command *remotev1.ControllerCommand) (CommandTranscript, error) {
	session.dispatchMu.Lock()
	defer session.dispatchMu.Unlock()
	select {
	case <-session.done:
		return CommandTranscript{}, ErrSessionClosed
	default:
	}
	if !session.ready {
		return CommandTranscript{}, errors.New("controller inventory is not settled")
	}
	if command == nil || command.GetCommandId() == "" {
		return CommandTranscript{}, errors.New("command ID is required")
	}
	command = proto.Clone(command).(*remotev1.ControllerCommand)
	if command.GetConnectionEpoch() == 0 {
		command.ConnectionEpoch = session.hello.GetConnectionEpoch()
	}
	if command.GetSequence() == 0 {
		command.Sequence = session.lastSequence + 1
	}
	if command.GetConnectionEpoch() != session.hello.GetConnectionEpoch() {
		return CommandTranscript{}, errors.New("command connection epoch does not match the current session")
	}
	if command.GetSequence() != session.lastSequence+1 {
		return CommandTranscript{}, errors.New("command sequence is not the next claimed sequence")
	}
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.After(time.Now()) || ctx.Err() != nil {
		return CommandTranscript{}, errors.New("command context requires a future deadline")
	}
	command.TimeoutMillis = uint64(max(1, time.Until(deadline).Milliseconds()))
	if err := session.stream.Send(&remotev1.ConnectResponse{Payload: &remotev1.ConnectResponse_Command{Command: command}}); err != nil {
		session.terminate(err)
		return CommandTranscript{}, fmt.Errorf("%w: %v", ErrSessionClosed, err)
	}
	session.lastSequence = command.GetSequence()

	transcript := CommandTranscript{}
	unknown := func(cause error) (CommandTranscript, error) {
		transcript.Result = unknownResult(command, session.watermark)
		return transcript, fmt.Errorf("%w: %v", ErrUnknownOutcome, cause)
	}
	protocolFailure := func(cause error) (CommandTranscript, error) {
		session.terminate(cause)
		return unknown(cause)
	}
	for {
		var message *remotev1.ConnectRequest
		select {
		case message = <-session.messages:
		case <-session.done:
			select {
			case message = <-session.messages:
			default:
				return unknown(session.Err())
			}
		case <-ctx.Done():
			return unknown(ctx.Err())
		}
		var sequence uint64
		switch payload := message.GetPayload().(type) {
		case *remotev1.ConnectRequest_Ack:
			sequence = payload.Ack.GetSequence()
		case *remotev1.ConnectRequest_Evidence:
			sequence = payload.Evidence.GetSequence()
		case *remotev1.ConnectRequest_Result:
			sequence = payload.Result.GetSequence()
		}
		if sequence < command.GetSequence() {
			continue
		}
		switch payload := message.GetPayload().(type) {
		case *remotev1.ConnectRequest_Ack:
			if err := validateIdentity(command, payload.Ack.GetCommandId(), payload.Ack.GetSequence(), payload.Ack.GetConnectionEpoch()); err != nil {
				return protocolFailure(err)
			}
			transcript.Ack = payload.Ack
			if payload.Ack.GetDisposition() == remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED {
				transcript.Result = &remotev1.CommandResult{CommandId: command.GetCommandId(), Sequence: command.GetSequence(), ConnectionEpoch: command.GetConnectionEpoch(), Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_REJECTED, Reason: payload.Ack.GetReason(), Message: payload.Ack.GetMessage(), InventoryWatermark: session.watermark}
				return transcript, nil
			}
		case *remotev1.ConnectRequest_Evidence:
			if transcript.Ack == nil {
				return protocolFailure(errors.New("execution evidence arrived before admission ACK"))
			}
			if err := validateIdentity(command, payload.Evidence.GetCommandId(), payload.Evidence.GetSequence(), payload.Evidence.GetConnectionEpoch()); err != nil {
				return protocolFailure(err)
			}
			transcript.Evidence = append(transcript.Evidence, payload.Evidence)
		case *remotev1.ConnectRequest_Result:
			if transcript.Ack == nil {
				return protocolFailure(errors.New("result arrived before admission ACK"))
			}
			if err := validateIdentity(command, payload.Result.GetCommandId(), payload.Result.GetSequence(), payload.Result.GetConnectionEpoch()); err != nil {
				return protocolFailure(err)
			}
			transcript.Result = payload.Result
			if payload.Result.GetInventoryWatermark() > session.watermark {
				session.watermark = payload.Result.GetInventoryWatermark()
			}
			return transcript, nil

		default:
			return protocolFailure(errors.New("unexpected host control message"))
		}
	}
}

func ValidateHello(hello *remotev1.HostHello, registrationID string) error {
	if hello == nil || hello.GetRegistrationId() == "" || hello.GetRegistrationId() != registrationID {
		return errors.New("invalid remote registration identity")
	}
	if err := validateProtocolVersion(hello.GetProtocolVersion()); err != nil {
		return err
	}
	if hello.GetConnectionEpoch() == 0 {
		return errors.New("remote connection contract is incomplete")
	}
	if len(hello.GetInventory()) > MaxActiveInventoryItems {
		return errors.New("remote inventory snapshot exceeds message limit")
	}
	seen := make(map[string]struct{}, len(hello.GetInventory()))
	for _, allocation := range hello.GetInventory() {
		if allocation.GetAllocationId() == "" {
			return errors.New("remote inventory contains an empty allocation ID")
		}
		if _, exists := seen[allocation.GetAllocationId()]; exists {
			return errors.New("remote inventory contains a duplicate allocation")
		}
		seen[allocation.GetAllocationId()] = struct{}{}
	}
	if hello.GetInventoryWatermark() == 0 {
		return errors.New("remote inventory watermark is required")
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
	return errors.Is(err, ErrUnknownOutcome)
}

func validateProtocolVersion(value string) error {
	version, err := strconv.ParseUint(value, 10, 64)
	if err == nil && version >= MinProtocolVersion && version <= ProtocolVersion {
		return nil
	}
	side := "too_old"
	if err == nil && version > ProtocolVersion {
		side = "too_new"
	}
	failure, _ := status.New(codes.FailedPrecondition, "Host protocol is outside the supported range").WithDetails(&errdetails.ErrorInfo{
		Domain: hostv1.ErrorDomain, Reason: "REMOTE_PROTOCOL_UNSUPPORTED",
		Metadata: map[string]string{"host_protocol": value, "min_protocol": strconv.Itoa(MinProtocolVersion), "max_protocol": strconv.Itoa(ProtocolVersion), "host_side": side},
	})
	return failure.Err()
}
