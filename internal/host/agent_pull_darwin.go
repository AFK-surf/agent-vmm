//go:build darwin

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
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/registryarchive"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errRegistryDestination = errors.New("image registry resolved to a non-public address")

func importRequired(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() == "IMAGE_IMPORT_REQUIRED" {
			return true
		}
	}
	return false
}

// importRegistryImage materializes a cold image: it fetches the linux/arm64
// image from its registry and commits the archive through the immutable
// ImportImage path. The derived request ID keeps a retried pull idempotent.
func (server *agentServer) importRegistryImage(ctx context.Context, identity auth.Identity, requestID, reference string) error {
	canonical, err := guestimage.NormalizeReference(reference)
	if err != nil {
		return resourceError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image", reference, "image reference is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, importDownloadTimeout)
	defer cancel()
	directory := filepath.Join(server.controller.instanceDirectory, "image-pulls")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	archive, err := registryarchive.Export(ctx, canonical, directory, maxImportArchiveBytes, server.controller.registryTransport())
	if err != nil {
		return registryPullError(ctx, err, canonical)
	}
	defer os.Remove(archive.Path)
	header := &hostv1.ImportImageHeader{RequestId: derivedRequestID(fmt.Sprintf("image-pull/%s/%s", identity.NamespaceID, requestID)), ImageReference: canonical,
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
	image := func(code codes.Code, reason, message string) error {
		return resourceError(code, reason, "image", reference, message)
	}
	var registry *transport.Error
	var network net.Error
	switch {
	case errors.Is(err, registryarchive.ErrTooLarge):
		return invalidField("reference", "image exceeds the 8 GiB import limit")
	case errors.Is(err, registryarchive.ErrPlatformUnavailable):
		return image(codes.FailedPrecondition, "IMAGE_PLATFORM_UNAVAILABLE", "image has no linux/arm64 variant")
	case errors.Is(err, errRegistryDestination):
		return image(codes.Unavailable, "IMAGE_REGISTRY_UNAVAILABLE", errRegistryDestination.Error())
	case errors.As(err, &registry) && registry.StatusCode == http.StatusTooManyRequests:
		return image(codes.Unavailable, "IMAGE_REGISTRY_RATE_LIMITED", "image registry rate limit reached")
	case errors.As(err, &registry) && (registry.StatusCode == http.StatusUnauthorized || registry.StatusCode == http.StatusForbidden):
		return image(codes.Unavailable, "IMAGE_AUTH_UNAVAILABLE", "image registry requires credentials that are unavailable")
	case errors.As(err, &registry) && registry.StatusCode == http.StatusNotFound && !errors.Is(err, registryarchive.ErrDownload):
		return image(codes.NotFound, "IMAGE_NOT_FOUND", "image reference does not exist")
	case ctx.Err() != nil || errors.As(err, &network) && network.Timeout():
		return image(codes.Unavailable, "IMAGE_DOWNLOAD_TIMEOUT", "image download exceeded its time limit")
	case errors.Is(err, registryarchive.ErrDownload):
		return image(codes.Unavailable, "IMAGE_DOWNLOAD_INTERRUPTED", "image download was interrupted")
	case errors.As(err, &registry):
		return image(codes.Unavailable, "IMAGE_MANIFEST_FETCH_FAILED", "image manifest could not be downloaded")
	default:
		return image(codes.Unavailable, "IMAGE_REGISTRY_UNAVAILABLE", "image registry could not be reached")
	}
}

func (controller *Controller) registryTransport() http.RoundTripper {
	if controller.registryRoundTripper != nil {
		return controller.registryRoundTripper
	}
	return publicRegistryTransport(importReceiveIdleTimeout)
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
