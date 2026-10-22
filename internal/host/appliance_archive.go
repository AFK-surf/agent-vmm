//go:build darwin || linux

package host

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

const applianceArchiveChunkBytes = 1 << 20

func (controller *Controller) importApplianceArchive(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, artifact store.ApplianceArtifact, grant store.ApplianceGrant) (resultErr error) {
	started := time.Now()
	log.Printf("appliance content preparation begin: installation=%s artifact=%s", installation.ID, artifact.ID)
	defer func() {
		log.Printf("appliance content preparation end: installation=%s elapsed=%s succeeded=%t", installation.ID, time.Since(started).Round(time.Millisecond), resultErr == nil)
	}()
	refs, err := artifactContentReferences(artifact)
	if err != nil {
		return err
	}
	path := ""
	var controllerContent *appliancev1.ApplianceContentReference
	for _, content := range refs {
		if err := controller.stageGuestApplianceContent(ctx, client, content); err != nil {
			return err
		}
		if content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER {
			path = filepath.Join(controller.applianceContentRoot(), appliancecontent.ArchiveName(content))
			controllerContent = content
		}
	}
	if controllerContent == nil {
		if strings.HasPrefix(artifact.ImageReference, "agent-vmm.local/mcp@") {
			return nil
		}
		return errors.New("custom appliance artifact requires a staged controller content reference")
	}
	archive, err := os.Open(path)
	if err != nil {
		return err
	}
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maxImportArchiveBytes {
		return errors.New("stored appliance archive has an invalid size")
	}
	archiveSHA256 := controllerContent.GetArchiveSha256()
	log.Printf("appliance controller import begin: installation=%s digest=%s archive_bytes=%d", installation.ID, artifact.ImageDigest, info.Size())
	importStarted := time.Now()
	defer func() {
		log.Printf("appliance controller import end: installation=%s elapsed=%s succeeded=%t", installation.ID, time.Since(importStarted).Round(time.Millisecond), resultErr == nil)
	}()
	stream, err := client.ImportImage(ctx)
	if err != nil {
		return err
	}
	header := &vmmdv1.ImportImageHeader{ActorNamespaceId: systemNamespaceID(installation.ID), ImageReference: artifact.ImageReference, ArchiveSha256: archiveSHA256, ManifestDigest: artifact.ImageDigest, Platform: artifact.Platform, ArchiveSize: uint64(info.Size()), SystemImport: true, SystemLimits: &vmmdv1.NamespaceLimits{Pids: uint32(grant.System.PIDs), DiskBytes: grant.System.DiskBytes}, SystemEgressMode: vmmdv1.EgressMode(grant.EgressMode)}
	header.SystemRetention, err = controller.applianceImageRetention(ctx, installation.ID)
	if err != nil {
		return err
	}
	header.RequestId, err = applianceImportRequestID(header)
	if err != nil {
		return err
	}
	_, completed, err := sendImportHeader(stream.Send, stream.CloseAndRecv, header)
	if err != nil || completed {
		return err
	}
	buffer := make([]byte, vmmdv1.MaxImportArchiveChunkBytes)
	for {
		count, readErr := archive.Read(buffer)
		if count > 0 {
			if err := stream.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: append([]byte(nil), buffer[:count]...)}}); err != nil {
				if errors.Is(err, io.EOF) {
					_, err = stream.CloseAndRecv()
				}
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
