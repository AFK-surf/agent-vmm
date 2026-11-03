package host

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

type curlArchiveFixture struct {
	raw      string
	endpoint importArchiveEndpoint
	command  importCurlCommand
	process  *exec.Cmd
}

func newCurlArchiveFixture(t *testing.T, handler http.HandlerFunc) *curlArchiveFixture {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	ca := filepath.Join(t.TempDir(), "archive-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	fixture := &curlArchiveFixture{raw: server.URL + "/archive", endpoint: importArchiveEndpoint{hostname: "127.0.0.1", port: port, address: netip.MustParseAddr("127.0.0.1")}}
	fixture.command = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		fixture.process = exec.CommandContext(ctx, name, append(args, "--cacert", ca)...)
		return fixture.process
	}
	return fixture
}

func (fixture *curlArchiveFixture) download(ctx context.Context, raw string, size uint64, send func([]byte) error) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return downloadImportArchiveWithCurl(ctx, raw, size, send, fixture.endpoint, fixture.command)
}

func (fixture *curlArchiveFixture) requireReaped(t *testing.T) {
	t.Helper()
	if fixture.process == nil || fixture.process.ProcessState == nil {
		t.Fatal("curl child was not waited")
	}
}

func TestCurlImportCommitsOnlySuccessfulExactArchive(t *testing.T) {
	t.Setenv("CURL_SSL_BACKEND", "unavailable-backend")
	t.Setenv("CURL_CA_BUNDLE", "/unavailable-ca")
	t.Setenv("SSL_CERT_FILE", "/unavailable-ca")
	for _, test := range []struct {
		name         string
		status, size int
		payload      []byte
		valid        bool
	}{
		{"complete", 200, 7 << 20, bytes.Repeat([]byte("a"), 7<<20), true},
		{"HTTP failure", 404, 7, []byte("archive"), false},
		{"redirect", 302, 7, []byte("archive"), false},
		{"short", 200, 8, []byte("archive"), false},
		{"long", 200, 6, []byte("archive"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var hits atomic.Int32
			fixture := newCurlArchiveFixture(t, func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				if test.status == 302 {
					w.Header().Set("Location", "/redirect-target")
				}
				w.WriteHeader(test.status)
				_, _ = w.Write(test.payload)
			})
			closes := 0
			var received bytes.Buffer
			_, err := forwardImportArchiveWithDownload(context.Background(), fixture.raw, uint64(test.size), func(request *vmmdv1.ImportImageRequest) error {
				chunk := request.GetArchiveChunk()
				if len(chunk) > vmmdv1.MaxImportArchiveChunkBytes {
					t.Fatalf("unbounded chunk %d", len(chunk))
				}
				_, err := received.Write(chunk)
				return err
			}, func() (*vmmdv1.Image, error) { closes++; return &vmmdv1.Image{}, nil }, fixture.download)
			if (err == nil) != test.valid || closes != map[bool]int{true: 1, false: 0}[test.valid] {
				t.Fatalf("error=%v commits=%d", err, closes)
			}
			if test.valid && !bytes.Equal(received.Bytes(), test.payload) {
				t.Fatal("curl changed archive bytes")
			}
			fixture.requireReaped(t)
			if hits.Load() != 1 {
				t.Fatalf("download issued %d requests", hits.Load())
			}
		})
	}
}

func TestCurlImportPreservesTLSHostnameVerification(t *testing.T) {
	fixture := newCurlArchiveFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("archive")) })
	// The fixture certificate permits 127.0.0.1, but not localhost.
	fixture.endpoint.hostname = "localhost"
	fixture.raw = "https://localhost:" + fixture.endpoint.port + "/archive"
	closes := 0
	_, err := forwardImportArchiveWithDownload(context.Background(), fixture.raw, 7, func(*vmmdv1.ImportImageRequest) error {
		t.Fatal("TLS refusal forwarded archive bytes")
		return nil
	}, func() (*vmmdv1.Image, error) { closes++; return nil, nil }, fixture.download)
	if err == nil || closes != 0 {
		t.Fatalf("TLS hostname refusal err=%v commits=%d", err, closes)
	}
	fixture.requireReaped(t)
}

func TestCurlImportCancellationReapsChild(t *testing.T) {
	started := make(chan struct{})
	fixture := newCurlArchiveFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fixture.download(ctx, fixture.raw, 7, func([]byte) error { return nil }) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("curl did not reach local origin")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel left curl running")
	}
	fixture.requireReaped(t)
}

func TestCurlImportStopsForGuestResultAndSendFailure(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(strconv.FormatBool(early), func(t *testing.T) {
			fixture := newCurlArchiveFixture(t, func(w http.ResponseWriter, r *http.Request) {
				payload := bytes.Repeat([]byte("a"), 1<<20)
				for index := 0; index < 16; index++ {
					if _, err := w.Write(payload); err != nil {
						return
					}
				}
			})
			wantErr := errors.New("guest send refused")
			wantImage := &vmmdv1.Image{Reference: "already-materialized"}
			closes := 0
			image, err := forwardImportArchiveWithDownload(context.Background(), fixture.raw, 16<<20, func(*vmmdv1.ImportImageRequest) error {
				if early {
					return io.EOF
				}
				return wantErr
			}, func() (*vmmdv1.Image, error) { closes++; return wantImage, nil }, fixture.download)
			if early {
				if err != nil || image != wantImage || closes != 1 {
					t.Fatalf("early image=%v closes=%d err=%v", image, closes, err)
				}
			} else if !errors.Is(err, wantErr) || closes != 0 {
				t.Fatalf("failure closes=%d err=%v", closes, err)
			}
			fixture.requireReaped(t)
		})
	}
}

func TestCurlImportRejectsPrivateResolutionBeforeProcess(t *testing.T) {
	started := false
	err := downloadImportArchiveWithCurlResolver(context.Background(), "https://archive.example/image.tar", 7, func([]byte) error { return nil }, fixedImportResolver{addresses: []netip.Addr{netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("127.0.0.1")}}, func(context.Context, string, ...string) *exec.Cmd { started = true; return nil })
	if err == nil || started {
		t.Fatalf("private DNS result err=%v curlStarted=%v", err, started)
	}
}
