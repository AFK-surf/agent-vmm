//go:build darwin

package host

import (
	"context"
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
	maxImportArchiveBytes = 8 << 30
	importDownloadTimeout = 10 * time.Minute
)

var errGuestImportCompleted = errors.New("guest image import completed")

func (server *agentServer) ImportImage(stream hostv1.AgentRuntimeService_ImportImageServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || header.GetRequestId() == "" || header.GetImageReference() == "" || len(header.GetArchiveSha256()) != 32 ||
		header.GetManifestDigest() == "" || header.GetPlatform() != "linux/arm64" || header.GetArchiveSize() == 0 || header.GetArchiveSize() > maxImportArchiveBytes {
		return invalidField("header", "complete bounded linux/arm64 OCI import metadata is required")
	}
	if err := validateImportArchiveURL(header.GetArchiveUrl()); err != nil {
		return invalidField("archive_url", err.Error())
	}
	if request, err := stream.Recv(); !errors.Is(err, io.EOF) || request != nil {
		return invalidField("archive", "runtime OCI bytes must be fetched from archive_url")
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.ImportImage(stream.Context())
	if err != nil {
		return err
	}
	guestHeader := importHeaderToGuest(header, identity.NamespaceID)
	image, completed, err := sendImportHeader(guest.Send, guest.CloseAndRecv, guestHeader)
	if err != nil {
		return err
	}
	if completed {
		return stream.SendAndClose(imageFromGuest(image))
	}
	image, err = forwardImportArchive(stream.Context(), header.GetArchiveUrl(), header.GetArchiveSize(), guest.Send, guest.CloseAndRecv)
	if err != nil {
		return err
	}
	return stream.SendAndClose(imageFromGuest(image))
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
		Proxy: nil,
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
