package servicepreface

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestListenerRejectsWrongCapabilityAndConsumesValidPreface(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	capability := []byte("0123456789abcdef0123456789abcdef")
	listener, err := New(base, capability)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, _ := listener.Accept()
		accepted <- connection
	}()
	write := func(value []byte, payload string) net.Conn {
		connection, dialErr := net.Dial("tcp", base.Addr().String())
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		header := append([]byte(Magic), make([]byte, 4)...)
		binary.BigEndian.PutUint32(header[len(Magic):], uint32(len(value)))
		_, _ = connection.Write(append(append(header, value...), payload...))
		return connection
	}
	wrong := write([]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), "forged")
	defer wrong.Close()
	valid := write(capability, "grpc-bytes")
	defer valid.Close()
	select {
	case connection := <-accepted:
		defer connection.Close()
		payload := make([]byte, len("grpc-bytes"))
		if _, err := io.ReadFull(connection, payload); err != nil || string(payload) != "grpc-bytes" {
			t.Fatalf("payload = %q, %v", payload, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid connection was not admitted")
	}
}
