//go:build linux

package control

import (
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestSystemStreamPurposeBounds(t *testing.T) {
	for _, kind := range []vmmdv1.SystemStreamKind{vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP, vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER, vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND} {
		t.Run(kind.String(), func(t *testing.T) {
			_, header := systemStreamFixture()
			header.Kind = kind
			if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND {
				header.Name = "http"
			}
			if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER {
				header.MaxTotalBytes = 0
			}
			if err := validateSystemStreamBounds(header); err != nil {
				t.Fatal(err)
			}
			if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND {
				for _, total := range []uint64{0, 2 << 30} {
					candidate := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
					candidate.MaxTotalBytes = total
					if err := validateSystemStreamBounds(candidate); err != nil {
						t.Fatalf("valid frontend total %d: %v", total, err)
					}
				}
			}
			for _, mutate := range []func(*vmmdv1.SystemStreamHeader){
				func(value *vmmdv1.SystemStreamHeader) {
					if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER {
						value.MaxTotalBytes = (1 << 30)
					} else if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP {
						value.MaxTotalBytes = 0
					} else {
						value.MaxTotalBytes = 1
					}
				},
				func(value *vmmdv1.SystemStreamHeader) { value.MaxFrameBytes = frontendFrameLimit + 1 },
				func(value *vmmdv1.SystemStreamHeader) { value.IdleSeconds = 0 },
				func(value *vmmdv1.SystemStreamHeader) { value.MaxConcurrentStreams = 17 },
			} {
				candidate := proto.Clone(header).(*vmmdv1.SystemStreamHeader)
				mutate(candidate)
				if err := validateSystemStreamBounds(candidate); err == nil {
					t.Fatal("purpose accepted invalid bounds")
				}
			}
		})
	}
}

func TestSystemStreamUnlimitedBeyondOldFrontendTotal(t *testing.T) {
	for _, kind := range []vmmdv1.SystemStreamKind{vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER, vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND} {
		t.Run(kind.String(), func(t *testing.T) {
			supervisor, header := systemStreamFixture()
			header.Kind = kind
			socket := "controller.sock"
			if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND {
				header.Name = "management"
				socket = "frontend-management.sock"
			}
			header.MaxFrameBytes, header.MaxTotalBytes, header.IdleSeconds = frontendFrameLimit, 0, 30
			path := t.TempDir()
			supervisor.records[header.GetIdentity().GetInstallationId()].runtimePath = path
			endpoint, err := net.Listen("unix", filepath.Join(path, socket))
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			echoDone := make(chan error, 1)
			go func() {
				conn, err := endpoint.Accept()
				if err != nil {
					echoDone <- err
					return
				}
				defer conn.Close()
				_, err = io.Copy(conn, conn)
				echoDone <- err
			}()
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			vmmdv1.RegisterVMMDServiceServer(server, &Server{appliances: supervisor})
			go server.Serve(listener)
			defer server.Stop()
			connection, err := grpc.NewClient("passthrough:///controller", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			stream, err := vmmdv1.NewVMMDServiceClient(connection).OpenSystemStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Header{Header: header}}); err != nil {
				t.Fatal(err)
			}
			data := bytesOf(42, frontendFrameLimit)
			sent := make(chan error, 1)
			go func() {
				for total := uint64(0); total <= (1 << 30); total += uint64(len(data)) {
					if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: data}}); err != nil {
						sent <- err
						return
					}
				}
				sent <- stream.CloseSend()
			}()
			var received uint64
			for {
				frame, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if frame.GetCloseWrite() {
					break
				}
				for _, value := range frame.GetData() {
					if value != 42 {
						t.Fatal("echo changed data")
					}
				}
				received += uint64(len(frame.GetData()))
			}
			if received != (1<<30)+frontendFrameLimit {
				t.Fatalf("received %d bytes", received)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			if err := <-echoDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

type observedSystemStreamServer struct {
	*Server
	completed chan error
	released  chan struct{}
	sends     atomic.Int32
}

func (server *observedSystemStreamServer) OpenSystemStream(stream grpc.BidiStreamingServer[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame]) error {
	err := server.Server.OpenSystemStream(&observedSystemStream{BidiStreamingServer: stream, owner: server})
	go func() {
		<-stream.Context().Done()
		deadline := time.Now().Add(time.Second)
		for server.sends.Load() != 0 {
			if time.Now().After(deadline) {
				return
			}
			time.Sleep(time.Millisecond)
		}
		close(server.released)
	}()
	server.completed <- err
	return err
}

type observedSystemStream struct {
	grpc.BidiStreamingServer[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame]
	owner *observedSystemStreamServer
}

func (stream *observedSystemStream) Send(frame *vmmdv1.SystemStreamFrame) error {
	stream.owner.sends.Add(1)
	defer stream.owner.sends.Add(-1)
	return stream.BidiStreamingServer.Send(frame)
}

func TestSystemStreamSlowReceiverAndCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			supervisor, header := systemStreamFixture()
			header.Kind = vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER
			header.MaxTotalBytes = 0
			header.MaxFrameBytes = 64 << 10
			header.IdleSeconds = 1
			path := t.TempDir()
			supervisor.records[header.GetIdentity().GetInstallationId()].runtimePath = path
			endpoint, err := net.Listen("unix", filepath.Join(path, "controller.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			wrote := make(chan struct{})
			writerDone := make(chan struct{})
			go func() {
				defer close(writerDone)
				conn, err := endpoint.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				data := make([]byte, 64<<10)
				close(wrote)
				for {
					if _, err := conn.Write(data); err != nil {
						return
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, completed, released := openObservedSystemStream(t, ctx, supervisor, header)
			select {
			case <-wrote:
			case <-time.After(time.Second):
				t.Fatal("endpoint not opened")
			}
			if cancelEarly {
				cancel()
			}
			select {
			case err := <-completed:
				if err == nil {
					t.Fatal("blocked stream succeeded")
				}
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("slow receiver exceeded idle bound")
			}
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("gRPC handler termination retained an in-flight Send")
			}
			select {
			case <-writerDone:
			case <-time.After(time.Second):
				t.Fatal("endpoint writer retained after stream termination")
			}
		})
	}
}

func openObservedSystemStream(t *testing.T, ctx context.Context, supervisor *applianceSupervisor, header *vmmdv1.SystemStreamHeader) (grpc.BidiStreamingClient[vmmdv1.SystemStreamFrame, vmmdv1.SystemStreamFrame], <-chan error, <-chan struct{}) {
	t.Helper()
	observed := &observedSystemStreamServer{Server: &Server{appliances: supervisor}, completed: make(chan error, 1), released: make(chan struct{})}
	listener := bufconn.Listen(64 << 10)
	server := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(server, observed)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bounded", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	stream, err := vmmdv1.NewVMMDServiceClient(connection).OpenSystemStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Header{Header: header}}); err != nil {
		t.Fatal(err)
	}
	return stream, observed.completed, observed.released
}

func TestBoundedSystemStreamRejectsInputBeforeEndpointWrite(t *testing.T) {
	for _, kind := range []vmmdv1.SystemStreamKind{vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP, vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND} {
		for _, frameExcess := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/frame_excess=%t", kind, frameExcess), func(t *testing.T) {
				supervisor, header := systemStreamFixture()
				header.Kind = kind
				header.MaxFrameBytes = 8
				header.MaxTotalBytes = 8
				header.IdleSeconds = 1
				filename := "bootstrap.sock"
				if kind == vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_FRONTEND {
					header.Name = "http"
					filename = "frontend-http.sock"
				}
				path := t.TempDir()
				supervisor.records[header.GetIdentity().GetInstallationId()].runtimePath = path
				endpoint, err := net.Listen("unix", filepath.Join(path, filename))
				if err != nil {
					t.Fatal(err)
				}
				defer endpoint.Close()
				received := make(chan []byte, 1)
				go func() {
					conn, err := endpoint.Accept()
					if err != nil {
						return
					}
					defer conn.Close()
					data, _ := io.ReadAll(conn)
					received <- data
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				stream, completed, _ := openObservedSystemStream(t, ctx, supervisor, header)
				data := make([]byte, 8)
				if frameExcess {
					data = append(data, 1)
				}
				if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: data}}); err != nil {
					t.Fatal(err)
				}
				if !frameExcess {
					if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: []byte{1}}}); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case err := <-completed:
					if err == nil {
						t.Fatal("over-limit input succeeded")
					}
				case <-ctx.Done():
					t.Fatal("over-limit input did not terminate")
				}
				select {
				case data := <-received:
					want := 8
					if frameExcess {
						want = 0
					}
					if len(data) != want {
						t.Fatalf("endpoint received %d bytes, want %d", len(data), want)
					}
				case <-time.After(time.Second):
					t.Fatal("endpoint retained after rejection")
				}
			})
		}
	}
}

func TestSystemStreamHalfCloseStillBoundsInputIdle(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(fmt.Sprint(progress), func(t *testing.T) {
			supervisor, header := systemStreamFixture()
			header.Kind = vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_CONTROLLER
			header.MaxTotalBytes = 0
			header.IdleSeconds = 1
			path := t.TempDir()
			supervisor.records[header.GetIdentity().GetInstallationId()].runtimePath = path
			endpoint, err := net.Listen("unix", filepath.Join(path, "controller.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			endpointDone := make(chan struct{})
			go func() {
				defer close(endpointDone)
				conn, err := endpoint.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.(*net.UnixConn).CloseWrite()
				_, _ = io.Copy(io.Discard, conn)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, completed, released := openObservedSystemStream(t, ctx, supervisor, header)
			frame, err := stream.Recv()
			if err != nil || !frame.GetCloseWrite() {
				t.Fatalf("half close=%v err=%v", frame, err)
			}
			if progress {
				for range 5 {
					time.Sleep(300 * time.Millisecond)
					if err := stream.Send(&vmmdv1.SystemStreamFrame{Payload: &vmmdv1.SystemStreamFrame_Data{Data: []byte{1}}}); err != nil {
						t.Fatal(err)
					}
				}
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-completed:
				if (err == nil) != progress {
					t.Fatalf("half-close progress=%t err=%v", progress, err)
				}
			case <-time.After(3 * time.Second):
				cancel()
				t.Fatal("half-closed stream exceeded input idle bound")
			}
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("half-close retained gRPC sender")
			}
			select {
			case <-endpointDone:
			case <-time.After(time.Second):
				t.Fatal("half-close retained endpoint reader")
			}
		})
	}
}
