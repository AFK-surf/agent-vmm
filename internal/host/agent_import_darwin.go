//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

const (
	maxImportArchiveBytes = 8 << 30
	maxImportChunkBytes   = 1 << 20
	importDownloadTimeout = 10 * time.Minute
)

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
	if err := downloadImportArchive(stream.Context(), header.GetArchiveUrl(), header.GetArchiveSize(), func(chunk []byte) error {
		return guest.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: chunk}})
	}); err != nil {
		return err
	}
	image, err = guest.CloseAndRecv()
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
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: importDownloadTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("runtime OCI download redirects are not allowed")
		},
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
	buffer := make([]byte, maxImportChunkBytes)
	var received uint64
	for {
		count, readErr := limited.Read(buffer)
		if count > 0 {
			received += uint64(count)
			if received > size {
				return errors.New("runtime OCI archive exceeds committed size")
			}
			if err := send(append([]byte(nil), buffer[:count]...)); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
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
