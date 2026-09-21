package remote_test

import (
	"context"
	"io"
	"testing"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
)

type scriptedSessionStream struct {
	received []*remotev1.HostSessionFrame
	sent     []*remotev1.ControllerSessionFrame
}

func (stream *scriptedSessionStream) Send(frame *remotev1.ControllerSessionFrame) error {
	stream.sent = append(stream.sent, frame)
	return nil
}

func (stream *scriptedSessionStream) Recv() (*remotev1.HostSessionFrame, error) {
	if len(stream.received) == 0 {
		return nil, io.EOF
	}
	frame := stream.received[0]
	stream.received = stream.received[1:]
	return frame, nil
}

func (*scriptedSessionStream) Context() context.Context { return context.Background() }

func TestAcceptSessionReturnsBoundedNetConn(t *testing.T) {
	t.Parallel()
	stream := &scriptedSessionStream{received: []*remotev1.HostSessionFrame{
		{Payload: &remotev1.HostSessionFrame_Header{Header: &remotev1.SessionHeader{RegistrationId: "registration-1", AllocationId: "allocation-1", AllocationGeneration: 2, ConnectionEpoch: 19}}},
		{Payload: &remotev1.HostSessionFrame_Data{Data: []byte("from-host")}},
	}}
	header, connection, err := controllerremote.AcceptSession(stream, "registration-1", 19)
	if err != nil {
		t.Fatal(err)
	}
	if header.GetAllocationGeneration() != 2 {
		t.Fatalf("header = %+v", header)
	}
	buffer := make([]byte, 16)
	count, err := connection.Read(buffer)
	if err != nil || string(buffer[:count]) != "from-host" {
		t.Fatalf("Read = %q, %v", buffer[:count], err)
	}
	if _, err := connection.Write([]byte("to-host")); err != nil {
		t.Fatal(err)
	}
	if got := string(stream.sent[0].GetData()); got != "to-host" {
		t.Fatalf("sent = %q", got)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.Done():
	default:
		t.Fatal("connection did not close")
	}
}
