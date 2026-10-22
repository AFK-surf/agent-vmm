//go:build darwin || linux

package host

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

const (
	maxImportArchiveBytes    = 8 << 30
	importDownloadTimeout    = 10 * time.Minute
	importReceiveIdleTimeout = 30 * time.Second
)

var errGuestImportCompleted = errors.New("guest image import completed")

func (server *agentServer) ImportImage(stream hostv1.AgentRuntimeService_ImportImageServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), importDownloadTimeout)
	defer cancel()
	identity, err := auth.AgentIdentity(ctx)
	if err != nil {
		return err
	}
	first, err := receiveImportRequest(ctx, stream.Recv, importReceiveIdleTimeout)
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetRequestId() == "" || header.GetImageReference() == "" || len(header.GetArchiveSha256()) != 32 ||
		header.GetManifestDigest() == "" || header.GetPlatform() != "linux/arm64" || header.GetArchiveSize() == 0 || header.GetArchiveSize() > maxImportArchiveBytes {
		return invalidField("header", "complete bounded linux/arm64 OCI import metadata is required")
	}
	if header.GetArchiveUrl() != "" {
		if err := validateImportArchiveURL(header.GetArchiveUrl()); err != nil {
			return invalidField("archive_url", err.Error())
		}
		if request, err := receiveImportRequest(ctx, stream.Recv, importReceiveIdleTimeout); !errors.Is(err, io.EOF) || request != nil {
			return invalidField("archive", "runtime OCI bytes must be fetched from archive_url")
		}
	}
	image, err := server.importToGuest(ctx, identity, header, func(send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), image *vmmdv1.Image, completed bool) (*vmmdv1.Image, error) {
		if header.GetArchiveUrl() == "" {
			return forwardLocalImportArchive(ctx, header, stream.Recv, send, closeAndRecv, image, completed)
		}
		if completed {
			return image, nil
		}
		return forwardImportArchive(ctx, header.GetArchiveUrl(), header.GetArchiveSize(), send, closeAndRecv)
	})
	if err != nil {
		return err
	}
	return stream.SendAndClose(imageFromGuest(image))
}

type importForward func(send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), image *vmmdv1.Image, completed bool) (*vmmdv1.Image, error)

// importToGuest is the one immutable import path: it fences the environment,
// sends the committed header, and lets forward supply the archive bytes.
func (server *agentServer) importToGuest(ctx context.Context, identity auth.Identity, header *hostv1.ImportImageHeader, forward importForward) (*vmmdv1.Image, error) {
	releaseInventory, err := server.controller.lockInventoryMutation(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	if err := server.controller.requireUndisposedEnvironment(ctx, identity.EnvironmentID); err != nil {
		return nil, err
	}
	unlock, err := server.controller.lockEnvironmentContext(ctx, identity.EnvironmentID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := server.validateAgentInventoryTarget(ctx, identity.EnvironmentID, identity.NamespaceID); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	guest, err := client.ImportImage(ctx)
	if err != nil {
		return nil, err
	}
	image, completed, err := sendImportHeader(guest.Send, guest.CloseAndRecv, importHeaderToGuest(header, identity.NamespaceID))
	if err != nil {
		return nil, err
	}
	return forward(guest.Send, guest.CloseAndRecv, image, completed)
}

// Each receive starts only after the previous chunk reaches the Guest.
func receiveImportRequest[T any](ctx context.Context, recv func() (*T, error), idle time.Duration) (*T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		request *T
		err     error
	}
	ready := make(chan result, 1)
	go func() { request, err := recv(); ready <- result{request, err} }()
	timer := time.NewTimer(idle)
	defer timer.Stop()
	select {
	case value := <-ready:
		return value.request, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, context.DeadlineExceeded
	}
}

func forwardLocalImportArchive(ctx context.Context, header *hostv1.ImportImageHeader, recv func() (*hostv1.ImportImageRequest, error), send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), image *vmmdv1.Image, completed bool) (*vmmdv1.Image, error) {
	hash := sha256.New()
	var received uint64
	for {
		request, err := receiveImportRequest(ctx, recv, importReceiveIdleTimeout)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		chunk, ok := request.GetPayload().(*hostv1.ImportImageRequest_ArchiveChunk)
		if !ok || len(chunk.ArchiveChunk) == 0 || len(chunk.ArchiveChunk) > vmmdv1.MaxImportArchiveChunkBytes || uint64(len(chunk.ArchiveChunk)) > header.GetArchiveSize()-received {
			return nil, invalidField("archive_chunk", "archive chunks must be non-empty, bounded, and within archive_size")
		}
		received += uint64(len(chunk.ArchiveChunk))
		_, _ = hash.Write(chunk.ArchiveChunk)
		if completed {
			continue
		}
		err = send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: chunk.ArchiveChunk}})
		if errors.Is(err, io.EOF) {
			image, err = closeAndRecv()
			completed = err == nil
		}
		if err != nil {
			return nil, err
		}
	}
	if received != header.GetArchiveSize() || !bytes.Equal(hash.Sum(nil), header.GetArchiveSha256()) {
		return nil, invalidField("archive", "OCI archive size or sha256 does not match the header")
	}
	if completed {
		return image, nil
	}
	return closeAndRecv()
}

func sendImportHeader(send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), header *vmmdv1.ImportImageHeader) (*vmmdv1.Image, bool, error) {
	err := send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_Header{Header: header}})
	if err == nil {
		return nil, false, nil
	}
	if !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	image, err := closeAndRecv()
	if err != nil {
		return nil, false, err
	}
	return image, true, nil
}

func forwardImportArchive(ctx context.Context, raw string, size uint64, send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error)) (*vmmdv1.Image, error) {
	return forwardImportArchiveWithDownload(ctx, raw, size, send, closeAndRecv, downloadImportArchive)
}

func forwardImportArchiveWithDownload(ctx context.Context, raw string, size uint64, send func(*vmmdv1.ImportImageRequest) error, closeAndRecv func() (*vmmdv1.Image, error), download func(context.Context, string, uint64, func([]byte) error) error) (*vmmdv1.Image, error) {
	var completed *vmmdv1.Image
	err := download(ctx, raw, size, func(chunk []byte) error {
		err := send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: chunk}})
		if !errors.Is(err, io.EOF) {
			return err
		}
		completed, err = closeAndRecv()
		if err != nil {
			return err
		}
		return errGuestImportCompleted
	})
	if errors.Is(err, errGuestImportCompleted) {
		return completed, nil
	}
	if err != nil {
		return nil, err
	}
	return closeAndRecv()
}

func validateImportArchiveURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("an absolute HTTPS URL without credentials, query, or fragment is required")
	}
	return nil
}

func downloadImportArchive(ctx context.Context, raw string, size uint64, send func([]byte) error) error {
	ctx, cancel := context.WithTimeout(ctx, importDownloadTimeout)
	defer cancel()
	client, err := importHTTPClient(ctx, raw, net.DefaultResolver, (&net.Dialer{}).DialContext)
	if err != nil {
		return fmt.Errorf("resolve runtime OCI archive: %w", err)
	}
	return downloadImportArchiveWithClient(ctx, client, raw, size, send)
}

type importAddressResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

func importHTTPClient(ctx context.Context, raw string, resolver importAddressResolver, dialContext func(context.Context, string, string) (net.Conn, error)) (*http.Client, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	hostname := parsed.Hostname()
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip4", hostname)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, errors.New("runtime OCI archive host has no public IPv4 address")
	}
	for _, address := range addresses {
		if !importArchiveAddressAllowed(address) {
			return nil, errors.New("runtime OCI archive host resolved to a non-public address")
		}
	}
	selected := addresses[0]
	transport := &http.Transport{
		Proxy: nil, ForceAttemptHTTP2: true,
		DialContext: func(dialCtx context.Context, network, requested string) (net.Conn, error) {
			requestedHost, requestedPort, splitErr := net.SplitHostPort(requested)
			if splitErr != nil || !strings.EqualFold(strings.TrimSuffix(requestedHost, "."), strings.TrimSuffix(hostname, ".")) || requestedPort != port {
				return nil, errors.New("runtime OCI archive transport requested an unexpected destination")
			}
			return dialContext(dialCtx, "tcp4", net.JoinHostPort(selected.String(), port))
		},
	}
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base.TLSClientConfig != nil {
		transport.TLSClientConfig = base.TLSClientConfig.Clone()
	}
	return &http.Client{
		Transport: transport,
		Timeout:   importDownloadTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("runtime OCI download redirects are not allowed")
		},
	}, nil
}

func downloadImportArchiveWithClient(ctx context.Context, client *http.Client, raw string, size uint64, send func([]byte) error) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download runtime OCI archive: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download runtime OCI archive: HTTP %d", response.StatusCode)
	}
	if response.ContentLength >= 0 && uint64(response.ContentLength) != size {
		return fmt.Errorf("runtime OCI archive content length = %d, want %d", response.ContentLength, size)
	}
	limited := io.LimitReader(response.Body, int64(size)+1)
	buffer := make([]byte, vmmdv1.MaxImportArchiveChunkBytes)
	var received uint64
	for {
		count, readErr := io.ReadFull(limited, buffer)
		if count > 0 {
			received += uint64(count)
			if received > size {
				return errors.New("runtime OCI archive exceeds committed size")
			}
			if err := send(append([]byte(nil), buffer[:count]...)); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read runtime OCI archive: %w", readErr)
		}
	}
	if received != size {
		return fmt.Errorf("runtime OCI archive size = %d, want %d", received, size)
	}
	return nil
}

func importHeaderToGuest(header *hostv1.ImportImageHeader, namespaceID string) *vmmdv1.ImportImageHeader {
	return &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: namespaceID, ImageReference: header.GetImageReference(),
		ArchiveSha256: append([]byte(nil), header.GetArchiveSha256()...), ManifestDigest: header.GetManifestDigest(), Platform: header.GetPlatform(), ArchiveSize: header.GetArchiveSize()}
}
