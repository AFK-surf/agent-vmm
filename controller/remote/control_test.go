package remote_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
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
		{Payload: &remotev1.HostControlMessage_EndpointObservationRenewal{EndpointObservationRenewal: &remotev1.EndpointObservationRenewal{ConnectionEpoch: 19, EndpointObservation: &trustv1.EndpointObservation{DeviceId: "device-a", DeviceSignature: []byte("signed")}}}},
		{Payload: &remotev1.HostControlMessage_FrontendEvent{FrontendEvent: &hostv1.HostLoopbackFrontendEvent{Kind: hostv1.HostLoopbackFrontendEvent_KIND_EXPIRED, FrontendLeaseId: "frontend-1", Generation: 2}}},
		{Payload: &remotev1.HostControlMessage_Evidence{Evidence: &remotev1.CommandEvidence{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_STARTED}}},
		{Payload: &remotev1.HostControlMessage_Result{Result: &remotev1.CommandResult{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Outcome: remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, InventoryWatermark: 8}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}})
	if err != nil {
		t.Fatal(err)
	}
	if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED || len(transcript.Evidence) != 1 {
		t.Fatalf("transcript = %+v", transcript)
	}
	select {
	case event := <-session.FrontendEvents():
		if event.GetFrontendLeaseId() != "frontend-1" || event.GetGeneration() != 2 {
			t.Fatalf("frontend event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("frontend event was not delivered")
	}
	if ready := stream.sent[0].GetControllerReady(); ready.GetRegistrationId() != "registration-1" || ready.GetConnectionEpoch() != 19 || ready.GetAcknowledgedInventoryWatermark() != 7 {
		t.Fatalf("controller ready = %+v", ready)
	}
	if len(stream.sent[0].GetControllerReady().GetEnabledFeatures()) != 0 {
		t.Fatalf("legacy host was granted optional features: %+v", stream.sent[0].GetControllerReady().GetEnabledFeatures())
	}
	command := stream.sent[1].GetCommand()
	if command.GetSequence() != 1 || command.GetConnectionEpoch() != 19 || command.GetDeadlineUnixMillis() == 0 {
		t.Fatalf("dispatched command = %+v", command)
	}
}

func TestControllerReadyNegotiatesRemoteObservationOnlyWithSupportingHost(t *testing.T) {
	t.Parallel()
	hello := validHello()
	hello.SupportedFeatures = append(hello.SupportedFeatures, controllerremote.RemoteObservationFeature)
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}}}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	features := stream.sent[0].GetControllerReady().GetEnabledFeatures()
	if len(features) != 1 || features[0] != controllerremote.RemoteObservationFeature {
		t.Fatalf("enabled features = %v", features)
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
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transcript, err := session.Dispatch(ctx, &remotev1.ControllerCommand{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}})
	if !errors.Is(err, controllerremote.ErrUnknownOutcome) {
		t.Fatalf("Dispatch error = %v", err)
	}
	if transcript.Result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_UNKNOWN {
		t.Fatalf("result = %+v", transcript.Result)
	}
}

func TestControlSessionRoutesFencedRemoteObservationsOutsideCommandTranscript(t *testing.T) {
	t.Parallel()
	hello := validHello()
	hello.SupportedFeatures = append(hello.SupportedFeatures, controllerremote.RemoteObservationFeature)
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: hello}},
		{Payload: &remotev1.HostControlMessage_ObservationRenewal{ObservationRenewal: &remotev1.RemoteObservationRenewal{ConnectionEpoch: 19, Observation: &remotev1.RemoteObservationSnapshot{Sequence: 2, ObservedUnixMillis: time.Now().UnixMilli(), InventoryObservedUnixMillis: time.Now().UnixMilli()}}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	select {
	case renewal := <-session.Observations():
		if renewal.GetConnectionEpoch() != 19 || renewal.GetObservation().GetSequence() != 2 {
			t.Fatalf("renewal = %+v", renewal)
		}
	case <-time.After(time.Second):
		t.Fatal("remote observation was not delivered")
	}
}

func TestControlSessionRejectsRemoteObservationWithoutNegotiation(t *testing.T) {
	t.Parallel()
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: validHello()}},
		{Payload: &remotev1.HostControlMessage_ObservationRenewal{ObservationRenewal: &remotev1.RemoteObservationRenewal{ConnectionEpoch: 19, Observation: &remotev1.RemoteObservationSnapshot{Sequence: 2, ObservedUnixMillis: time.Now().UnixMilli(), InventoryObservedUnixMillis: time.Now().UnixMilli()}}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Done():
		if err := session.Err(); err == nil || err.Error() != "remote observation feature was not negotiated" {
			t.Fatalf("session error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("unnegotiated remote observation did not fail the session")
	}
	if renewal, ok := <-session.Observations(); ok || renewal != nil {
		t.Fatalf("unnegotiated renewal was delivered: %+v", renewal)
	}
}

func TestControlSessionRejectsCommandBeforeReady(t *testing.T) {
	t.Parallel()
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: validHello()}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Dispatch(context.Background(), &remotev1.ControllerCommand{CommandId: "too-early", Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}); err == nil {
		t.Fatal("command was dispatched before ControllerReady")
	}
	if len(stream.sent) != 0 {
		t.Fatalf("pre-ready command reached host: %+v", stream.sent)
	}
}

func TestControlSessionRejectsUnclaimedCommandIdentity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		sequence uint64
		epoch    uint64
	}{
		{name: "missing sequence", epoch: 19},
		{name: "sequence gap", sequence: 2, epoch: 19},
		{name: "wrong epoch", sequence: 1, epoch: 18},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
				{Payload: &remotev1.HostControlMessage_Hello{Hello: validHello()}},
			}}
			session, err := controllerremote.AcceptControl(stream, "registration-1")
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Ready(); err != nil {
				t.Fatal(err)
			}
			command := &remotev1.ControllerCommand{CommandId: "command-1", Sequence: test.sequence, ConnectionEpoch: test.epoch, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
			if _, err := session.Dispatch(context.Background(), command); err == nil {
				t.Fatal("unclaimed command identity was dispatched")
			}
			if len(stream.sent) != 1 || stream.sent[0].GetControllerReady() == nil {
				t.Fatalf("command reached host: %+v", stream.sent)
			}
		})
	}
}

func TestControlSessionExpiredClaimDoesNotConsumeSequence(t *testing.T) {
	t.Parallel()
	stream := &scriptedControlStream{ctx: context.Background(), received: []*remotev1.HostControlMessage{
		{Payload: &remotev1.HostControlMessage_Hello{Hello: validHello()}},
		{Payload: &remotev1.HostControlMessage_Ack{Ack: &remotev1.CommandAck{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, Disposition: remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED, Reason: remotev1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT}}},
	}}
	session, err := controllerremote.AcceptControl(stream, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Ready(); err != nil {
		t.Fatal(err)
	}
	expired := &remotev1.ControllerCommand{CommandId: "expired", Sequence: 1, ConnectionEpoch: 19, DeadlineUnixMillis: time.Now().Add(-time.Minute).UnixMilli(), Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
	if _, err := session.Dispatch(context.Background(), expired); err == nil {
		t.Fatal("expired claimed command was dispatched")
	}
	if len(stream.sent) != 1 {
		t.Fatalf("expired command reached host: %+v", stream.sent)
	}

	valid := &remotev1.ControllerCommand{CommandId: "command-1", Sequence: 1, ConnectionEpoch: 19, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Command: &remotev1.ControllerCommand_DrainRegistration{DrainRegistration: &remotev1.DrainRegistration{}}}
	transcript, err := session.Dispatch(context.Background(), valid)
	if err != nil {
		t.Fatal(err)
	}
	if transcript.Ack.GetDisposition() != remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED || len(stream.sent) != 2 {
		t.Fatalf("valid retry = %+v sent=%+v", transcript, stream.sent)
	}
}

func TestValidateHelloRejectsFeatureAndInventoryDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*remotev1.HostHello)
	}{
		{name: "feature", mutate: func(hello *remotev1.HostHello) { hello.RequiredFeatures = []string{"unknown"} }},
		{name: "unnegotiated observation", mutate: func(hello *remotev1.HostHello) { hello.Observation = &remotev1.RemoteObservationSnapshot{} }},
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
	hello := validHello()
	hello.SupportedFeatures = append(hello.SupportedFeatures, controllerremote.RemoteObservationFeature)
	hello.Observation = &remotev1.RemoteObservationSnapshot{}
	if err := controllerremote.ValidateHello(hello, "registration-1"); err != nil {
		t.Fatalf("ValidateHello rejected negotiated observation: %v", err)
	}
	hello = validHello()
	hello.EndpointObservation = &trustv1.EndpointObservation{}
	if err := controllerremote.ValidateHello(hello, "registration-1"); err != nil {
		t.Fatalf("ValidateHello coupled legacy endpoint observation to remote observation negotiation: %v", err)
	}
	hello = validHello()
	hello.Inventory = nil
	for index := 0; index < 65; index++ {
		hello.Inventory = append(hello.Inventory, &remotev1.Allocation{AllocationId: fmt.Sprintf("allocation-%d", index)})
	}
	if err := controllerremote.ValidateHello(hello, "registration-1"); err == nil {
		t.Fatal("ValidateHello accepted inventory above the end-to-end bound")
	}
}

func validHello() *remotev1.HostHello {
	features := append([]string(nil), controllerremote.RequiredFeatures...)
	return &remotev1.HostHello{ProtocolVersion: controllerremote.ProtocolVersion, HostApiVersion: "host.v1", RegistrationId: "registration-1", ConnectionEpoch: 19, InventoryWatermark: 7, SupportedFeatures: features, RequiredFeatures: append([]string(nil), features...), Inventory: []*remotev1.Allocation{{AllocationId: "allocation-1", Revision: 7}}}
}
