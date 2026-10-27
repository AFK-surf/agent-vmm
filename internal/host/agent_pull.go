//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/imageref"
	"github.com/AFK-surf/agent-vmm/internal/registryarchive"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc/codes"
)

var errRegistryDestination = errors.New("image registry resolved to a non-public address")

func importRequired(err error) bool {
	reason, ok := hostv1.ErrorReasonOf(err)
	return ok && reason == hostv1.ErrorReason_ERROR_REASON_IMAGE_IMPORT_REQUIRED
}

// importRegistryImage materializes a cold image: it fetches the linux/arm64
// image from its registry and commits the archive through the immutable
// ImportImage path. The derived request ID keeps a retried pull idempotent.
func (server *agentServer) importRegistryImage(ctx context.Context, identity auth.Identity, requestID, reference string) error {
	canonical, err := imageref.NormalizeReference(reference)
	if err != nil {
		return hostv1.NewError(codes.InvalidArgument, hostv1.ErrorReason_ERROR_REASON_IMAGE_REFERENCE_INVALID, "image reference is invalid", hostv1.WithResource("image", reference))
	}
	ctx, cancel := context.WithTimeout(ctx, guestconn.ImportDownloadTimeout)
	defer cancel()
	directory := filepath.Join(server.controller.instanceDirectory, "image-pulls")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	archive, err := registryarchive.Export(ctx, canonical, directory, guestconn.MaxImportArchiveBytes, registryTransport(server.controller.registryRoundTripper))
	if err != nil {
		return registryPullError(ctx, err, canonical)
	}
	defer os.Remove(archive.Path)
	header := &hostv1.ImportImageHeader{RequestId: opreceipt.DerivedRequestID(fmt.Sprintf("image-pull/%s/%s", identity.NamespaceID, requestID)), ImageReference: canonical,
		ArchiveSha256: archive.SHA256, ManifestDigest: archive.ManifestDigest, Platform: "linux/arm64", ArchiveSize: archive.Size}
	_, err = server.importToGuest(ctx, identity, header, func(send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), image *vmmdv1.Image, completed bool) (*vmmdv1.Image, error) {
		if completed {
			return image, nil
		}
		return forwardImportArchiveWithDownload(ctx, archive.Path, archive.Size, send, closeAndRecv, sendArchiveFile)
	})
	return err
}

func sendArchiveFile(_ context.Context, path string, _ uint64, send func([]byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	buffer := make([]byte, vmmdv1.MaxImportArchiveChunkBytes)
	for {
		count, readErr := io.ReadFull(file, buffer)
		if count > 0 {
			if err := send(append([]byte(nil), buffer[:count]...)); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func registryPullError(ctx context.Context, err error, reference string) error {
	image := func(code codes.Code, reason hostv1.ErrorReason, message string) error {
		return hostv1.NewError(code, reason, message, hostv1.WithResource("image", reference))
	}
	var registry *transport.Error
	var network net.Error
	switch {
	case errors.Is(err, registryarchive.ErrTooLarge):
		return hostv1.InvalidField("reference", "image exceeds the 8 GiB import limit")
	case errors.Is(err, registryarchive.ErrPlatformUnavailable):
		return image(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_PLATFORM_UNAVAILABLE, "image has no linux/arm64 variant")
	case errors.Is(err, errRegistryDestination):
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_REGISTRY_UNAVAILABLE, errRegistryDestination.Error())
	case errors.As(err, &registry) && registry.StatusCode == http.StatusTooManyRequests:
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_REGISTRY_RATE_LIMITED, "image registry rate limit reached")
	case errors.As(err, &registry) && (registry.StatusCode == http.StatusUnauthorized || registry.StatusCode == http.StatusForbidden):
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_AUTH_UNAVAILABLE, "image registry requires credentials that are unavailable")
	case errors.As(err, &registry) && registry.StatusCode == http.StatusNotFound && !errors.Is(err, registryarchive.ErrDownload):
		return image(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_IMAGE_NOT_FOUND, "image reference does not exist")
	case ctx.Err() != nil || errors.As(err, &network) && network.Timeout():
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_DOWNLOAD_TIMEOUT, "image download exceeded its time limit")
	case errors.Is(err, registryarchive.ErrDownload):
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_DOWNLOAD_INTERRUPTED, "image download was interrupted")
	case errors.As(err, &registry):
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_MANIFEST_FETCH_FAILED, "image manifest could not be downloaded")
	default:
		return image(codes.Unavailable, hostv1.ErrorReason_ERROR_REASON_IMAGE_REGISTRY_UNAVAILABLE, "image registry could not be reached")
	}
}

func registryTransport(transport http.RoundTripper) http.RoundTripper {
	if transport != nil {
		return transport
	}
	return publicRegistryTransport(guestconn.ImportReceiveIdleTimeout)
}

// publicRegistryTransport dials only public registry addresses, checking every
// DNS result of every hop, including redirects to blob storage. It bypasses
// proxy settings like runtime archive downloads. A read that makes no progress
// for idle fails the download.
func publicRegistryTransport(idle time.Duration) http.RoundTripper {
	dialer := &net.Dialer{Timeout: idle}
	transport := &http.Transport{
		Proxy: nil, ForceAttemptHTTP2: true, TLSHandshakeTimeout: idle, ResponseHeaderTimeout: idle,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
			if err != nil {
				return nil, err
			}
			if len(addresses) == 0 {
				return nil, errRegistryDestination
			}
			for _, candidate := range addresses {
				if !importArchiveAddressAllowed(candidate) {
					return nil, errRegistryDestination
				}
			}
			connection, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort(addresses[0].String(), port))
			if err != nil {
				return nil, err
			}
			return &idleConn{Conn: connection, idle: idle}, nil
		},
	}
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base.TLSClientConfig != nil {
		transport.TLSClientConfig = base.TLSClientConfig.Clone()
	}
	return transport
}

type idleConn struct {
	net.Conn
	idle time.Duration
}

func (connection *idleConn) Read(data []byte) (int, error) {
	if err := connection.Conn.SetReadDeadline(time.Now().Add(connection.idle)); err != nil {
		return 0, err
	}
	return connection.Conn.Read(data)
}
