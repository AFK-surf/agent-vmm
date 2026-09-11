//go:build darwin

package host

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
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
			Body:          io.NopCloser(bytes.NewReader(payload)),
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
		netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("127.0.0.1"),
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
