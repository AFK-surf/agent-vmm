//go:build linux

package control

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failingResolver struct {
	err   error
	calls int
}

func TestRegistryDownloadErrorClassifiesSafeFailure(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		code    codes.Code
		reason  string
		message string
	}{
		{name: "rate limited", err: errors.New("unexpected status from HEAD request: 429 Too Many Requests"), code: codes.ResourceExhausted, reason: "IMAGE_REGISTRY_RATE_LIMITED", message: "image registry rate limit was reached"},
		{name: "timeout", err: context.DeadlineExceeded, code: codes.Unavailable, reason: "IMAGE_DOWNLOAD_TIMEOUT", message: "image download timed out"},
		{name: "authorization", err: errors.New("failed to authorize: failed to fetch anonymous token: EOF"), code: codes.Unavailable, reason: "IMAGE_AUTH_UNAVAILABLE", message: "image registry authentication was interrupted"},
		{name: "transfer", err: io.ErrUnexpectedEOF, code: codes.Unavailable, reason: "IMAGE_DOWNLOAD_INTERRUPTED", message: "image download was interrupted"},
		{name: "other", err: errors.New("content verification failed"), code: codes.Unavailable, reason: "IMAGE_BLOB_FETCH_FAILED", message: "image content could not be downloaded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := registryDownloadError(test.err, "docker.io/example/image:latest")
			value := status.Convert(err)
			if value.Code() != test.code || value.Message() != test.message {
				t.Fatalf("registryDownloadError() = %v", err)
			}
			var reason string
			for _, detail := range value.Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok {
					reason = info.Reason
				}
			}
			if reason != test.reason {
				t.Fatalf("reason = %q, want %q", reason, test.reason)
			}
			if strings.Contains(value.Message(), test.err.Error()) {
				t.Fatalf("unsafe source error leaked: %q", value.Message())
			}
		})
	}
}

func (resolver *failingResolver) Resolve(context.Context, string) (string, ocispec.Descriptor, error) {
	resolver.calls++
	return "", ocispec.Descriptor{}, resolver.err
}

func (*failingResolver) Fetcher(context.Context, string) (remotes.Fetcher, error) {
	panic("unexpected Fetcher call")
}

func (*failingResolver) Pusher(context.Context, string) (remotes.Pusher, error) {
	panic("unexpected Pusher call")
}

func TestRemoteImageSizeRetriesTransientResolutionFailure(t *testing.T) {
	var resolvers []*failingResolver
	factory := func() (*http.Client, remotes.Resolver) {
		resolver := &failingResolver{err: errors.New("temporary registry failure")}
		resolvers = append(resolvers, resolver)
		return new(http.Client), resolver
	}
	_, err := remoteImageSize(context.Background(), "docker.io/library/test:latest", factory)
	if status.Code(err) != codes.Unavailable || len(resolvers) != 3 {
		t.Fatalf("remoteImageSize() = %v with %d resolvers", err, len(resolvers))
	}
	for _, resolver := range resolvers {
		if resolver.calls != 1 {
			t.Fatalf("resolver calls = %d", resolver.calls)
		}
	}
}

func TestRemoteImageSizeDoesNotRetryMissingImage(t *testing.T) {
	resolver := &failingResolver{err: errdefs.ErrNotFound}
	created := 0
	_, err := remoteImageSize(context.Background(), "docker.io/library/missing:latest", func() (*http.Client, remotes.Resolver) {
		created++
		return new(http.Client), resolver
	})
	if status.Code(err) != codes.NotFound || resolver.calls != 1 {
		t.Fatalf("remoteImageSize() = %v after %d calls", err, resolver.calls)
	}
	if created != 1 {
		t.Fatalf("resolver factories = %d", created)
	}
}

func TestRemoteImageSizePreservesCancellationOnThirdAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	created := 0
	_, err := remoteImageSize(ctx, "docker.io/library/test:latest", func() (*http.Client, remotes.Resolver) {
		created++
		if created == 3 {
			cancel()
		}
		return new(http.Client), &failingResolver{err: errors.New("temporary registry failure")}
	})
	if !errors.Is(err, context.Canceled) || created != 3 {
		t.Fatalf("remoteImageSize() = %v after %d attempts", err, created)
	}
}

func TestRegistryRetryPreservesCancellationOnThirdPullAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := retryRegistry(ctx, retryableRegistryError, func(attempt int) error {
		attempts = attempt
		if attempt == 3 {
			cancel()
		}
		return errors.New("temporary registry failure")
	})
	if !errors.Is(err, context.Canceled) || attempts != 3 {
		t.Fatalf("retryRegistry() = %v after %d attempts", err, attempts)
	}
}
