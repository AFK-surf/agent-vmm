//go:build darwin

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestForwardImportArchiveForwardsGuestFailureAfterHeader(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("archive"))
	}))
	defer server.Close()
	want := errors.New("guest rejected import reservation")
	_, err := forwardImportArchiveWithDownload(
		context.Background(),
		server.URL,
		7,
		func(*vmmdv1.ImportImageRequest) error { return io.EOF },
		func() (*vmmdv1.Image, error) { return nil, want },
		func(ctx context.Context, raw string, size uint64, send func([]byte) error) error {
			return downloadImportArchiveWithClient(ctx, server.Client(), raw, size, send)
		},
	)
	if !errors.Is(err, want) {
		t.Fatalf("forwardImportArchive error = %v, want %v", err, want)
	}
}

func TestSendImportHeaderForwardsImmediateGuestResult(t *testing.T) {
	want := &vmmdv1.Image{Reference: "cue.local/runtime/external:release"}
	image, completed, err := sendImportHeader(
		func(request *vmmdv1.ImportImageRequest) error {
			if request.GetHeader().GetImageReference() != want.GetReference() {
				t.Fatalf("header reference = %q", request.GetHeader().GetImageReference())
			}
			return io.EOF
		},
		func() (*vmmdv1.Image, error) { return want, nil },
		&vmmdv1.ImportImageHeader{ImageReference: want.GetReference()},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !completed || image != want {
		t.Fatalf("completed = %v, image = %+v", completed, image)
	}
}

func TestImportImageHeaderInjectsSessionNamespaceAndPreservesContentCommitment(t *testing.T) {
	hash := make([]byte, 32)
	hash[0] = 7
	header := &hostv1.ImportImageHeader{RequestId: "request", ImageReference: "example.com/agent/pi:latest", ArchiveSha256: hash,
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Platform: "linux/arm64", ArchiveSize: 4096,
		ArchiveUrl: "https://cue-release.example/runtime/sha256/archive"}
	guest := importHeaderToGuest(header, "session-namespace")
	if guest.GetActorNamespaceId() != "session-namespace" || guest.GetRequestId() != header.GetRequestId() || guest.GetImageReference() != header.GetImageReference() ||
		guest.GetManifestDigest() != header.GetManifestDigest() || guest.GetPlatform() != "linux/arm64" || guest.GetArchiveSize() != header.GetArchiveSize() || guest.GetArchiveSha256()[0] != 7 {
		t.Fatalf("guest import header = %+v", guest)
	}
	header.ArchiveSha256[0] = 9
	if guest.GetArchiveSha256()[0] != 7 {
		t.Fatal("guest content commitment aliases the caller buffer")
	}
}

func TestValidateImportArchiveURL(t *testing.T) {
	if err := validateImportArchiveURL("https://cue-release.example/runtime/sha256/archive"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "http://cue-release.example/archive", "https://user@cue-release.example/archive", "https://cue-release.example/archive?token=mutable", "https://cue-release.example/archive#fragment"} {
		if err := validateImportArchiveURL(value); err == nil {
			t.Fatalf("validateImportArchiveURL(%q) succeeded", value)
		}
	}
}

func TestDownloadImportArchiveRequiresExactCommittedSize(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("archive"))
	}))
	defer server.Close()
	var received strings.Builder
	if err := downloadImportArchiveWithClient(context.Background(), server.Client(), server.URL, 7, func(chunk []byte) error {
		_, err := received.Write(chunk)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if received.String() != "archive" {
		t.Fatalf("downloaded %q", received.String())
	}
	if err := downloadImportArchiveWithClient(context.Background(), server.Client(), server.URL, 6, func([]byte) error { return nil }); err == nil {
		t.Fatal("oversized archive succeeded")
	}

	payload := bytes.Repeat([]byte("a"), 7<<20)
	largeClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: int64(len(payload)),
			Body:          io.NopCloser(&boundedReadReader{reader: bytes.NewReader(payload), max: 16 << 10}),
		}, nil
	})}
	var chunkSizes []int
	if err := downloadImportArchiveWithClient(context.Background(), largeClient, "https://archive.example/image.tar", uint64(len(payload)), func(chunk []byte) error {
		chunkSizes = append(chunkSizes, len(chunk))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(chunkSizes) != 3 || chunkSizes[0] != vmmdv1.MaxImportArchiveChunkBytes || chunkSizes[1] != vmmdv1.MaxImportArchiveChunkBytes || chunkSizes[2] != 1<<20 {
		t.Fatalf("archive chunk sizes = %v", chunkSizes)
	}
}

type boundedReadReader struct {
	reader io.Reader
	max    int
}

func (reader *boundedReadReader) Read(buffer []byte) (int, error) {
	if len(buffer) > reader.max {
		buffer = buffer[:reader.max]
	}
	return reader.reader.Read(buffer)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type fixedImportResolver struct {
	addresses []netip.Addr
}

func (resolver fixedImportResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return resolver.addresses, nil
}

func TestImportHTTPClientRejectsAnyNonPublicResolutionBeforeDial(t *testing.T) {
	dialed := false
	_, err := importHTTPClient(context.Background(), "https://archive.example/image.tar", fixedImportResolver{addresses: []netip.Addr{
		netip.MustParseAddr("198.18.76.40"), netip.MustParseAddr("127.0.0.1"),
	}}, func(context.Context, string, string) (net.Conn, error) {
		dialed = true
		return nil, errors.New("unexpected dial")
	})
	if err == nil || dialed {
		t.Fatalf("non-public DNS result: err=%v dialed=%v", err, dialed)
	}
}

func TestImportHTTPClientPinsValidatedAddressAndDisablesProxy(t *testing.T) {
	var network, destination string
	client, err := importHTTPClient(context.Background(), "https://archive.example:8443/image.tar", fixedImportResolver{addresses: []netip.Addr{
		netip.MustParseAddr("93.184.216.34"),
	}}, func(_ context.Context, gotNetwork, gotDestination string) (net.Conn, error) {
		network, destination = gotNetwork, gotDestination
		return nil, errors.New("dial observed")
	})
	if err != nil {
		t.Fatal(err)
	}
	transport := client.Transport.(*http.Transport)
	if transport.Proxy != nil {
		t.Fatal("runtime OCI transport inherited an HTTP proxy")
	}
	_, err = transport.DialContext(context.Background(), "tcp", "archive.example:8443")
	if err == nil || network != "tcp4" || destination != "93.184.216.34:8443" {
		t.Fatalf("pinned dial: network=%q destination=%q err=%v", network, destination, err)
	}
}

func TestImportArchiveThroughTransparentProxy(t *testing.T) {
	payload := []byte("published runtime archive")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "example.com" {
			t.Errorf("archive hostname changed: %s", r.Host)
		}
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	client, err := importHTTPClient(context.Background(), "https://example.com/image.tar",
		fixedImportResolver{addresses: []netip.Addr{netip.MustParseAddr("198.18.76.40")}},
		func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != "198.18.76.40:443" {
				t.Errorf("unvalidated destination: %s", address)
			}
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		})
	if err != nil {
		t.Fatal(err)
	}
	transport := client.Transport.(*http.Transport)
	transport.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	defer transport.CloseIdleConnections()
	var received bytes.Buffer
	if err := downloadImportArchiveWithClient(context.Background(), client, "https://example.com/image.tar", uint64(len(payload)), func(chunk []byte) error { _, err := received.Write(chunk); return err }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received.Bytes(), payload) {
		t.Fatalf("archive bytes changed: %q", received.Bytes())
	}
}

func TestForwardLocalImportArchive(t *testing.T) {
	payload := []byte("archive")
	digest := sha256.Sum256(payload)
	chunk := func(value []byte) *hostv1.ImportImageRequest {
		return &hostv1.ImportImageRequest{Payload: &hostv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: value}}
	}
	for _, test := range []struct {
		name                       string
		requests                   []*hostv1.ImportImageRequest
		replay, early, fail, valid bool
	}{
		{name: "normal", requests: []*hostv1.ImportImageRequest{chunk(payload)}, valid: true},
		{name: "replay", requests: []*hostv1.ImportImageRequest{chunk(payload)}, replay: true, valid: true},
		{name: "early result", requests: []*hostv1.ImportImageRequest{chunk(payload[:3]), chunk(payload[3:])}, early: true, valid: true},
		{name: "guest rejection", requests: []*hostv1.ImportImageRequest{chunk(payload)}, fail: true},
		{name: "truncated replay", replay: true},
		{name: "truncated", requests: []*hostv1.ImportImageRequest{chunk(payload[:3])}},
		{name: "excess", requests: []*hostv1.ImportImageRequest{chunk(append(payload, 'x'))}},
		{name: "empty", requests: []*hostv1.ImportImageRequest{chunk(nil)}},
		{name: "duplicate header", requests: []*hostv1.ImportImageRequest{{Payload: &hostv1.ImportImageRequest_Header{Header: &hostv1.ImportImageHeader{}}}}},
		{name: "bad hash", requests: []*hostv1.ImportImageRequest{chunk([]byte("changed"))}},
		{name: "bad hash after early result", requests: []*hostv1.ImportImageRequest{chunk([]byte("changed"))}, early: true},
		{name: "bad hash replay", requests: []*hostv1.ImportImageRequest{chunk([]byte("changed"))}, replay: true},
		{name: "chunk bound", requests: []*hostv1.ImportImageRequest{chunk(make([]byte, vmmdv1.MaxImportArchiveChunkBytes+1))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			index, sends, closes := 0, 0, 0
			want := &vmmdv1.Image{Reference: "image"}
			header := &hostv1.ImportImageHeader{ArchiveSize: uint64(len(payload)), ArchiveSha256: digest[:]}
			image, err := forwardLocalImportArchive(context.Background(), header, func() (*hostv1.ImportImageRequest, error) {
				if index == len(test.requests) {
					return nil, io.EOF
				}
				request := test.requests[index]
				index++
				return request, nil
			}, func(*vmmdv1.ImportImageRequest) error {
				sends++
				if test.early || test.fail {
					return io.EOF
				}
				return nil
			}, func() (*vmmdv1.Image, error) {
				closes++
				if test.fail {
					return nil, errors.New("guest rejected")
				}
				return want, nil
			}, want, test.replay)
			if (err == nil) != test.valid {
				t.Fatalf("image=%v err=%v", image, err)
			}
			if test.valid && image != want {
				t.Fatalf("image=%v", image)
			}
			if test.replay && (sends != 0 || closes != 0) {
				t.Fatalf("replay sent=%d closed=%d", sends, closes)
			}
			if test.early && (index != len(test.requests) || closes != 1 || sends != 1) {
				t.Fatalf("early result consumed=%d closed=%d sent=%d", index, closes, sends)
			}
		})
	}
}

func TestReceiveImportRequestBoundsWait(t *testing.T) {
	for _, test := range []struct {
		name   string
		cancel bool
	}{{"idle", false}, {"cancel", true}} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			defer close(release)
			if test.cancel {
				cancel()
			}
			_, err := receiveImportRequest(ctx, func() (*hostv1.ImportImageRequest, error) { <-release; return nil, io.EOF }, time.Millisecond)
			want := context.DeadlineExceeded
			if test.cancel {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
		})
	}
}

type importReceiveServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
	idle     time.Duration
	started  chan struct{}
	received chan error
	returned chan error
}

func (server *importReceiveServer) ImportImage(stream grpc.ClientStreamingServer[hostv1.ImportImageRequest, hostv1.Image]) error {
	_, err := receiveImportRequest(stream.Context(), func() (*hostv1.ImportImageRequest, error) {
		close(server.started)
		request, err := stream.Recv()
		server.received <- err
		return request, err
	}, server.idle)
	server.returned <- err
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return err
}

func TestReceiveImportRequestGRPCReleasesBlockedReceive(t *testing.T) {
	for _, test := range []struct {
		name   string
		idle   time.Duration
		cancel bool
		code   codes.Code
	}{
		{name: "cancel", idle: time.Minute, cancel: true, code: codes.Canceled},
		{name: "idle", idle: 50 * time.Millisecond, code: codes.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			serverImpl := &importReceiveServer{idle: test.idle, started: make(chan struct{}), received: make(chan error, 1), returned: make(chan error, 1)}
			server := grpc.NewServer()
			hostv1.RegisterAgentRuntimeServiceServer(server, serverImpl)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			connection, err := grpc.NewClient("passthrough:///import", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { connection.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := hostv1.NewAgentRuntimeServiceClient(connection).ImportImage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-serverImpl.started:
			case <-time.After(5 * time.Second):
				t.Fatal("server did not enter Recv")
			}
			if test.cancel {
				cancel()
			}
			err = stream.RecvMsg(&hostv1.Image{})
			if status.Code(err) != test.code {
				t.Fatalf("RPC error=%v want=%v", err, test.code)
			}
			select {
			case err := <-serverImpl.returned:
				if status.Code(err) != test.code && status.Code(status.FromContextError(err).Err()) != test.code {
					t.Fatalf("handler error=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not return")
			}
			select {
			case err := <-serverImpl.received:
				if err == nil {
					t.Fatal("blocked Recv returned no error")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Recv did not exit after RPC termination")
			}
		})
	}
}
