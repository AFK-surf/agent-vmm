//go:build darwin || linux

package host

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localForwardGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	target  string
	port    uint32
	streams sync.WaitGroup
	active  atomic.Int64
	peak    atomic.Int64
}

func (guest *localForwardGuest) ForwardTCP(stream grpc.BidiStreamingServer[vmmdv1.ForwardTCPRequest, vmmdv1.ForwardTCPResponse]) error {
	guest.streams.Add(1)
	defer guest.streams.Done()
	count := guest.active.Add(1)
	defer guest.active.Add(-1)
	for {
		old := guest.peak.Load()
		if count <= old || guest.peak.CompareAndSwap(old, count) {
			break
		}
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header.GetActorNamespaceId() != "service-ns" || header.GetContainerId() != "web" || header.GetContainerInstanceId() != "instance" || header.GetPort() != guest.port {
		return status.Error(codes.PermissionDenied, "wrong target")
	}
	peer, err := net.Dial("tcp", guest.target)
	if err != nil {
		return err
	}
	defer peer.Close()
	stop := context.AfterFunc(stream.Context(), func() { peer.Close() })
	defer stop()
	sent := make(chan error, 1)
	go func() {
		buffer := make([]byte, 64<<10)
		for {
			n, err := peer.Read(buffer)
			if n > 0 {
				if err := stream.Send(&vmmdv1.ForwardTCPResponse{Event: &vmmdv1.ForwardTCPResponse_Data{Data: buffer[:n]}}); err != nil {
					sent <- err
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					err = stream.Send(&vmmdv1.ForwardTCPResponse{Event: &vmmdv1.ForwardTCPResponse_CloseRead{CloseRead: true}})
				}
				sent <- err
				return
			}
		}
	}()
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return <-sent
		}
		if err != nil {
			peer.Close()
			<-sent
			return err
		}
		switch event := frame.Event.(type) {
		case *vmmdv1.ForwardTCPRequest_Data:
			if _, err := peer.Write(event.Data); err != nil {
				peer.Close()
				<-sent
				return err
			}
		case *vmmdv1.ForwardTCPRequest_CloseWrite:
			peer.(*net.TCPConn).CloseWrite()
		}
	}
}

func TestLocalServiceForwardRangeConcurrentSlowReadersAndWebSocket(t *testing.T) {
	content := bytes.Repeat([]byte("service-data-"), 256<<10)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			http.ServeContent(w, r, "sample", time.Unix(0, 0), bytes.NewReader(content))
			return
		}
		connection, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		accept := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		fmt.Fprintf(buffer, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(accept[:]))
		buffer.Flush()
		var header [2]byte
		if _, err := io.ReadFull(buffer, header[:]); err != nil {
			return
		}
		var mask [4]byte
		if _, err := io.ReadFull(buffer, mask[:]); err != nil {
			return
		}
		payload := make([]byte, int(header[1]&127))
		if _, err := io.ReadFull(buffer, payload); err != nil {
			return
		}
		for index := range payload {
			payload[index] ^= mask[index%4]
		}
		buffer.Write([]byte{0x81, byte(len(payload))})
		buffer.Write(payload)
		buffer.Flush()
		io.Copy(io.Discard, buffer)
	}))
	defer upstream.Close()
	target := upstream.Listener.Addr().String()
	_, portText, _ := net.SplitHostPort(target)
	port, _ := strconv.ParseUint(portText, 10, 16)
	guest := &localForwardGuest{target: target, port: uint32(port)}
	client := serviceRecoveryClient(t, guest)
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service-ns", DisplayName: "Service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value, err := database.CreateServiceListener(ctx, "open", &hostv1.ServiceListener{Id: "listener", Local: true, EnvironmentId: "service", ContainerId: "web", TargetPort: uint32(port), Exposure: hostv1.ServiceListenerExposure_SERVICE_LISTENER_EXPOSURE_DEVICE_LOOPBACK, ListenAddress: listener.Addr().String()}, &trustv1.RouteCapability{}, &trustv1.EndpointObservation{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &serviceListenerRuntime{listener: listener, cancel: cancel, done: make(chan struct{}), instance: "instance"}
	controller := &Controller{store: database, serviceListeners: map[string]*serviceListenerRuntime{value.Id: runtime}, workloadRuns: map[string]*workloadRun{workloadRunKey("service", "web"): {instanceID: "instance"}}}
	defer controller.closeAllServiceListeners()
	go func() {
		defer close(runtime.done)
		for {
			local, err := listener.Accept()
			if err != nil {
				return
			}
			if !runtime.track(local) {
				local.Close()
				return
			}
			go func() {
				defer runtime.release(local)
				defer local.Close()
				_ = controller.forwardLocalService(ctx, local, value, "instance", client)
			}()
		}
	}()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	url := "http://" + listener.Addr().String()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	request.Header.Set("Range", "bytes=123-2048")
	response, err := httpClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	ranged, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusPartialContent || !bytes.Equal(ranged, content[123:2049]) {
		t.Fatalf("Range status=%d bytes=%d error=%v", response.StatusCode, len(ranged), err)
	}
	baselineFD, baselineRSS := localForwardProcessResources(t)
	var readers sync.WaitGroup
	ready := make(chan struct{}, 6)
	read := make(chan struct{})
	for range 6 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			response, err := httpClient.Get(url)
			if err != nil {
				ready <- struct{}{}
				t.Error(err)
				return
			}
			defer response.Body.Close()
			ready <- struct{}{}
			<-read
			var body bytes.Buffer
			buffer := make([]byte, 32<<10)
			for {
				n, err := response.Body.Read(buffer)
				body.Write(buffer[:n])
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				time.Sleep(time.Millisecond)
			}
			if !bytes.Equal(body.Bytes(), content) {
				t.Error("slow reader changed payload")
			}
		}()
	}
	for range 6 {
		<-ready
	}
	concurrentFD, concurrentRSS := localForwardProcessResources(t)
	concurrentStreams := guest.active.Load()
	close(read)
	readers.Wait()
	socket, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	socket.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(socket, "GET /ws HTTP/1.1\r\nHost: service\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	buffer := bufio.NewReader(socket)
	upgrade, err := http.ReadResponse(buffer, &http.Request{Method: http.MethodGet})
	if err != nil || upgrade.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade=%v err=%v", upgrade, err)
	}
	message := []byte("echo")
	mask := []byte{1, 2, 3, 4}
	frame := append([]byte{0x81, 0x80 | byte(len(message))}, mask...)
	for index, value := range message {
		frame = append(frame, value^mask[index%4])
	}
	socket.Write(frame)
	echoed := make([]byte, len(message)+2)
	if _, err := io.ReadFull(buffer, echoed); err != nil || !bytes.Equal(echoed, append([]byte{0x81, byte(len(message))}, message...)) {
		t.Fatal("WebSocket payload changed", echoed, err)
	}
	if err := database.RevokeServiceListener(ctx, value.Id, value.Revision); err != nil {
		t.Fatal(err)
	}
	controller.stopServiceListener(value.Id)
	if _, err := buffer.ReadByte(); err == nil {
		t.Fatal("revoked stream stayed open")
	}
	guest.streams.Wait()
	if guest.active.Load() != 0 {
		t.Fatal("Guest streams remained open")
	}
	settledFD, settledRSS := localForwardProcessResources(t)
	t.Logf("forwarded %d bytes: baseline FD=%d RSS=%d KiB; six slow readers FD=%d RSS=%d KiB Guest streams=%d (peak=%d); settled FD=%d RSS=%d KiB Guest streams=%d", 6*len(content), baselineFD, baselineRSS, concurrentFD, concurrentRSS, concurrentStreams, guest.peak.Load(), settledFD, settledRSS, guest.active.Load())
}

func localForwardProcessResources(t *testing.T) (int, uint64) {
	t.Helper()
	pid := strconv.Itoa(os.Getpid())
	directory, err := os.Open("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	files, err := directory.Readdirnames(-1)
	directory.Close()
	if err != nil {
		t.Fatal(err)
	}
	descriptors := len(files)
	resident, err := exec.Command("ps", "-o", "rss=", "-p", pid).Output()
	if err != nil {
		t.Fatal(err)
	}
	rss, err := strconv.ParseUint(strings.TrimSpace(string(resident)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return descriptors, rss
}
