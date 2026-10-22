//go:build darwin || linux

package host

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/registryarchive"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

func TestRegistryPullFailuresMapToImageReasons(t *testing.T) {
	backend := registry.New()
	// Handlers of an earlier case can still run while the next case starts.
	var mode atomic.Value
	mode.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		isBlob := strings.Contains(request.URL.Path, "/blobs/")
		mode := mode.Load().(string)
		switch {
		case mode == "rate" && request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/manifests/"):
			writer.WriteHeader(http.StatusTooManyRequests)
		case mode == "auth" && strings.Contains(request.URL.Path, "/manifests/"):
			writer.WriteHeader(http.StatusUnauthorized)
		case mode == "stall" && isBlob && request.Method == http.MethodGet:
			<-request.Context().Done()
		default:
			backend.ServeHTTP(writer, request)
		}
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	push := func(repository, architecture string) string {
		image, err := random.Image(1024, 1)
		if err != nil {
			t.Fatal(err)
		}
		config, err := image.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		config = config.DeepCopy()
		config.OS, config.Architecture = "linux", architecture
		if image, err = mutate.ConfigFile(image, config); err != nil {
			t.Fatal(err)
		}
		ref, err := name.ParseReference(host + "/" + repository + ":test")
		if err != nil {
			t.Fatal(err)
		}
		if err := remote.Write(ref, image); err != nil {
			t.Fatal(err)
		}
		return ref.String()
	}
	arm64, amd64 := push("arm64", "arm64"), push("amd64", "amd64")
	for _, test := range []struct {
		mode, reference, reason string
		transport               http.RoundTripper
	}{
		{"", amd64, "IMAGE_PLATFORM_UNAVAILABLE", http.DefaultTransport},
		{"", host + "/missing:test", "IMAGE_NOT_FOUND", http.DefaultTransport},
		{"rate", arm64, "IMAGE_REGISTRY_RATE_LIMITED", http.DefaultTransport},
		{"auth", arm64, "IMAGE_AUTH_UNAVAILABLE", http.DefaultTransport},
		{"stall", arm64, "IMAGE_DOWNLOAD_TIMEOUT", http.DefaultTransport},
		// The product transport refuses the loopback registry before it dials.
		{"", arm64, "IMAGE_REGISTRY_UNAVAILABLE", publicRegistryTransport(time.Second)},
	} {
		t.Run(test.reason, func(t *testing.T) {
			mode.Store(test.mode)
			// The registry client retries 429 with its bounded default backoff.
			timeout := 30 * time.Second
			if test.mode == "stall" {
				timeout = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_, err := registryarchive.Export(ctx, test.reference, t.TempDir(), maxImportArchiveBytes, test.transport)
			if err == nil {
				t.Fatal("export succeeded")
			}
			if reason := pullReason(registryPullError(ctx, err, test.reference)); reason != test.reason {
				t.Fatalf("reason = %q, want %q (cause %v)", reason, test.reason, err)
			}
		})
	}
}

func TestIdleConnFailsAStalledRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &idleConn{Conn: connection, idle: 200 * time.Millisecond}, nil
	}}
	response, err := (&http.Client{Transport: transport}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	started := time.Now()
	if _, err := io.ReadAll(response.Body); err == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("stalled body read = %v after %s", err, time.Since(started))
	}
}

func pullReason(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return status.Convert(err).Message()
}
