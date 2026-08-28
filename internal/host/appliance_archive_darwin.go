//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

const applianceArchiveChunkBytes = 1 << 20

func (controller *Controller) applianceArchivePath(artifact store.ApplianceArtifact) string {
	return filepath.Join(controller.supportDirectory, "appliance-artifacts", hex.EncodeToString(artifact.CanonicalHash)+".oci.tar")
}

func (controller *Controller) importApplianceArchive(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, artifact store.ApplianceArtifact, grant store.ApplianceGrant) error {
	path := controller.applianceArchivePath(artifact)
	archive, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil // Guest bundles retain their own preinstalled first-party appliance image.
	}
	if err != nil {
		return err
	}
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maxImportArchiveBytes {
		return errors.New("stored appliance archive has an invalid size")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, archive); err != nil {
		return err
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return err
	}
	stream, err := client.ImportImage(ctx)
	if err != nil {
		return err
	}
	header := &vmmdv1.ImportImageHeader{ActorNamespaceId: systemNamespaceID(installation.ID), ImageReference: artifact.ImageReference, ArchiveSha256: hash.Sum(nil), ManifestDigest: artifact.ImageDigest, Platform: artifact.Platform, ArchiveSize: uint64(info.Size()), SystemImport: true, SystemLimits: &vmmdv1.NamespaceLimits{CpuMillis: uint32(grant.System.CPUMillis), MemoryBytes: grant.System.MemoryBytes, Pids: uint32(grant.System.PIDs), DiskBytes: grant.System.DiskBytes}, SystemEgressMode: vmmdv1.EgressMode(grant.EgressMode)}
	header.RequestId, err = applianceImportRequestID(header)
	if err != nil {
		return err
	}
	if err := stream.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_Header{Header: header}}); err != nil {
		return err
	}
	buffer := make([]byte, applianceArchiveChunkBytes)
	for {
		count, readErr := archive.Read(buffer)
		if count > 0 {
			if err := stream.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: append([]byte(nil), buffer[:count]...)}}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

func applianceImportRequestID(header *vmmdv1.ImportImageHeader) (string, error) {
	request := proto.Clone(header).(*vmmdv1.ImportImageHeader)
	request.RequestId = ""
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return "", err
	}
	return derivedRequestID(string(encoded)), nil
}
