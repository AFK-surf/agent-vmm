package remote_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

type liveControlStream struct {
	ctx      context.Context
	incoming chan *remotev1.ConnectRequest
	outgoing chan *remotev1.ConnectResponse
}

func (stream *liveControlStream) Context() context.Context { return stream.ctx }
func (stream *liveControlStream) Send(message *remotev1.ConnectResponse) error {
	select {
	case stream.outgoing <- message:
		return nil
	case <-stream.ctx.Done():
		return stream.ctx.Err()
	}
}
func (stream *liveControlStream) Recv() (*remotev1.ConnectRequest, error) {
	select {
	case message := <-stream.incoming:
		if message == nil {
			return nil, io.EOF
		}
		return message, nil
	case <-stream.ctx.Done():
		return nil, stream.ctx.Err()
	}
}
func liveSession(t *testing.T, enabled ...string) (*controllerremote.ControlSession, *liveControlStream) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := &liveControlStream{ctx: ctx, incoming: make(chan *remotev1.ConnectRequest, 256), outgoing: make(chan *remotev1.ConnectResponse, 8)}
	hello := validHello()
	hello.SupportedFeatures = append(hello.SupportedFeatures, controllerremote.RemoteObservationFeature)
	stream.incoming <- &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Hello{Hello: hello}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(enabled...); err != nil {
		t.Fatal(err)
	}
	<-stream.outgoing
	return session, stream
}
func commandAck(id string, sequence uint64) *remotev1.ConnectRequest {
	return &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Ack{Ack: &remotev1.CommandAck{CommandId: id, Sequence: sequence, ConnectionEpoch: 19, Disposition: remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED}}}
}
func commandResult(id string, sequence uint64) *remotev1.ConnectRequest {
	return &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Result{Result: &remotev1.CommandResult{CommandId: id, Sequence: sequence, ConnectionEpoch: 19, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 8}}}
}
func TestControlSessionCanceledCommandKeepsSessionAndDiscardsLateFrames(t *testing.T) {
	t.Parallel()
	session, stream := liveSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	result := make(chan error, 1)
	go func() {
		transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "first"})
		if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN {
			result <- errors.New("missing unknown result")
			return
		}
		result <- err
	}()
	command := (<-stream.outgoing).GetCommand()
	if command.GetSequence() != 1 || command.GetConnectionEpoch() != 19 {
		t.Fatalf("command = %v", command)
	}
	cancel()
	if err := <-result; !errors.Is(err, controllerremote.ErrUnknownOutcome) {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
		t.Fatal("cancellation ended the session")
	default:
	}
	stream.incoming <- commandAck("first", 1)
	stream.incoming <- commandResult("first", 1)
	go func() {
		<-stream.outgoing
		stream.incoming <- commandAck("second", 2)
		stream.incoming <- commandResult("second", 2)
	}()
	next, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	transcript, err := session.Dispatch(next, &remotev1.ControllerCommand{CommandId: "second"})
	if err != nil || transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		t.Fatalf("transcript=%v error=%v", transcript, err)
	}
	if session.Hello().GetInventoryWatermark() != 7 {
		t.Fatal("hello snapshot changed")
	}
}
func TestControlSessionLatestObservationAndEndpointRenewals(t *testing.T) {
	t.Parallel()
	session, stream := liveSession(t, controllerremote.RemoteObservationFeature)
	for sequence := uint64(2); sequence <= 10; sequence++ {
		stream.incoming <- &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_ObservationRenewal{ObservationRenewal: &remotev1.RemoteObservationRenewal{ConnectionEpoch: 19, Observation: &remotev1.RemoteObservationSnapshot{Sequence: sequence, ObservedUnixMillis: 1, InventoryObservedUnixMillis: 1}}}}
	}
	for range 129 {
		stream.incoming <- &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_EndpointObservationRenewal{EndpointObservationRenewal: &remotev1.EndpointObservationRenewal{ConnectionEpoch: 19, EndpointObservation: &trustv1.EndpointObservation{}}}}
	}
	// EOF follows every renewal, so Done confirms that the receiver consumed them.
	stream.incoming <- nil
	<-session.Done()
	if !errors.Is(session.Err(), io.EOF) {
		t.Fatal(session.Err())
	}
	if observation := <-session.Observations(); observation.GetObservation().GetSequence() != 10 {
		t.Fatalf("observation=%v", observation)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "closed"}); !errors.Is(err, controllerremote.ErrSessionClosed) {
		t.Fatal(err)
	}
	if len(stream.outgoing) != 0 {
		t.Fatal("closed session sent a command")
	}
	if controllerremote.IsUnknownOutcome(io.EOF) {
		t.Fatal("EOF is not a command outcome")
	}
}
func TestControlSessionExpiredContextDoesNotConsumeSequence(t *testing.T) {
	t.Parallel()
	session, stream := liveSession(t)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := session.Dispatch(expired, &remotev1.ControllerCommand{CommandId: "expired"}); err == nil || controllerremote.IsUnknownOutcome(err) {
		t.Fatalf("error=%v", err)
	}
	go func() {
		command := (<-stream.outgoing).GetCommand()
		stream.incoming <- commandAck(command.GetCommandId(), command.GetSequence())
		stream.incoming <- commandResult(command.GetCommandId(), command.GetSequence())
	}()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "current"})
	if err != nil || transcript.Ack.GetSequence() != 1 {
		t.Fatalf("transcript=%v error=%v", transcript, err)
	}
}
