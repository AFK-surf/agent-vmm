//go:build darwin

package host

import (
	"errors"
	"io"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

const (
	maxImportArchiveBytes = 8 << 30
	maxImportChunkBytes   = 1 << 20
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
	if err := guest.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_Header{Header: guestHeader}}); err != nil {
		if errors.Is(err, io.EOF) {
			_, err = guest.CloseAndRecv()
		}
		return err
	}
	var received uint64
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if received != header.GetArchiveSize() {
				return invalidField("archive_size", "archive stream ended before the committed size")
			}
			image, err := guest.CloseAndRecv()
			if err != nil {
				return err
			}
			return stream.SendAndClose(imageFromGuest(image))
		}
		if err != nil {
			return err
		}
		chunk, ok := request.GetPayload().(*hostv1.ImportImageRequest_ArchiveChunk)
		if !ok || len(chunk.ArchiveChunk) == 0 || len(chunk.ArchiveChunk) > maxImportChunkBytes || uint64(len(chunk.ArchiveChunk)) > header.GetArchiveSize()-received {
			return invalidField("archive_chunk", "archive chunks must be non-empty, bounded, and within the committed size")
		}
		received += uint64(len(chunk.ArchiveChunk))
		if err := guest.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: chunk.ArchiveChunk}}); err != nil {
			if errors.Is(err, io.EOF) {
				_, err = guest.CloseAndRecv()
			}
			return err
		}
	}
}

func importHeaderToGuest(header *hostv1.ImportImageHeader, namespaceID string) *vmmdv1.ImportImageHeader {
	return &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: namespaceID, ImageReference: header.GetImageReference(),
		ArchiveSha256: append([]byte(nil), header.GetArchiveSha256()...), ManifestDigest: header.GetManifestDigest(), Platform: header.GetPlatform(), ArchiveSize: header.GetArchiveSize()}
}
