package remote_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

type scriptedControlStream struct {
	ctx      context.Context
	received []*remotev1.HostControlMessage
	sent     []*remotev1.ControllerControlMessage
	err      error
}

func (stream *scriptedControlStream) Send(message *remotev1.ControllerControlMessage) error {
	stream.sent = append(stream.sent, message)
	return nil
}

func (stream *scriptedControlStream) Recv() (*remotev1.HostControlMessage, error) {
	if len(stream.received) == 0 {
		if stream.err != nil {
			return nil, stream.err
		}
		return nil, io.EOF
	}
	message := stream.received[0]
	stream.received = stream.received[1:]
	return message, nil
}

func (stream *scriptedControlStream) Context() context.Context { return stream.ctx }

func TestControlSessionDispatchTranscript(t *testing.T) {
	t.Parallel()
	hello := validHello()
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}},
		{Payload: &remotev1.HostControlMessage_Ack{Ack: &remotev1.CommandAck{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Disposition: remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED}}},
		{Payload: &remotev1.HostControlMessage_Evidence{Evidence: &remotev1.CommandEvidence{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_STARTED}}},
		{Payload: &remotev1.HostControlMessage_Result{Result: &remotev1.CommandResult{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 8}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "command-1", Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}})
	if err != nil {
		t.Fatal(err)
	}
	if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED || len(transcript.Evidence) != 1 {
		t.Fatalf("transcript = %+v", transcript)
	}
	command := stream.sent[0].GetCommand()
	if command.GetSequence() != 1 || command.GetConnectionEpoch() != 19 || command.GetDeadlineUnixMillis() == 0 {
		t.Fatalf("dispatched command = %+v", command)
	}
}

func TestControlSessionClassifiesLostResultUnknown(t *testing.T) {
	t.Parallel()
	stream := &scriptedControlStream{ctx: context.Background(), err: io.EOF, received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: validHello()}},
		{Payload: &remotev1.HostControlMessage_Ack{Ack: &remotev1.CommandAck{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Disposition: remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_ADMITTED}}},
		{Payload: &remotev1.HostControlMessage_Evidence{Evidence: &remotev1.CommandEvidence{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_STARTED}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "command-1", Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}})
	if !errors.Is(err, controllerremote.ErrUnknownOutcome) {
		t.Fatalf("Dispatch error = %v", err)
	}
	if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN {
		t.Fatalf("result = %+v", transcript.Result)
	}
}

func TestValidateHelloRejectsFeatureAndInventoryDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*remotev1.HostHello)
	}{
		{name: "feature", mutate: func(hello *remotev1.HostHello) { hello.RequiredFeatures = []string{"unknown"} }},
		{name: "epoch", mutate: func(hello *remotev1.HostHello) { hello.ConnectionEpoch = 0 }},
		{name: "watermark", mutate: func(hello *remotev1.HostHello) { hello.InventoryWatermark = 0 }},
		{name: "duplicate", mutate: func(hello *remotev1.HostHello) { hello.Inventory = append(hello.Inventory, hello.Inventory[0]) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hello := validHello()
			test.mutate(hello)
			if err := controllerremote.ValidateHello(hello, "registration-1"); err == nil {
				t.Fatal("ValidateHello accepted invalid hello")
			}
		})
	}
}

func validHello() *remotev1.HostHello {
	features := append([]string(nil), controllerremote.RequiredFeatures...)
	return &remotev1.HostHello{ProtocolVersion: "1", HostApiVersion: "host.v1", RegistrationId: "registration-1", ConnectionEpoch: 19, InventoryWatermark: 7, SupportedFeatures: features, RequiredFeatures: append([]string(nil), features...), Inventory: []*remotev1.Allocation{{AllocationId: "allocation-1", Revision: 7}}}
}
