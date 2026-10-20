//go:build darwin

package host

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func artifactContentReferences(artifact store.ApplianceArtifact) ([]*appliancev1.ApplianceContentReference, error) {
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return nil, err
	}
	return manifest.GetContentRefs(), nil
}

func (controller *Controller) stageGuestApplianceContent(ctx context.Context, client vmmdv1.VMMDServiceClient, content *appliancev1.ApplianceContentReference) (resultErr error) {
	started := time.Now()
	var sent uint64
	log.Printf("appliance Guest content stage begin: role=%s digest=%s archive_bytes=%d", content.GetRole(), content.GetImageManifestDigest(), content.GetArchiveSize())
	defer func() {
		log.Printf("appliance Guest content stage end: role=%s digest=%s sent_bytes=%d elapsed=%s succeeded=%t", content.GetRole(), content.GetImageManifestDigest(), sent, time.Since(started).Round(time.Millisecond), resultErr == nil)
	}()
	root := controller.applianceContentRoot()
	available, err := appliancecontent.Available(root, content)
	if err != nil {
		return err
	}
	if !available {
		return errors.New("approved appliance archive is not staged on the Host")
	}
	archive, err := os.Open(filepath.Join(root, appliancecontent.ArchiveName(content)))
	if err != nil {
		return err
	}
	defer archive.Close()
	stream, err := client.StageApplianceContent(ctx)
	if err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(importDownloadTimeout)
	}
	header := &appliancev1.StageContentHeader{RequestId: derivedRequestID(appliancecontent.ArchiveName(content)), DeadlineUnixMillis: deadline.UnixMilli(), ContentRef: content}
	err = stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_Header{Header: header}})
	if errors.Is(err, io.EOF) {
		result, receiptErr := stream.CloseAndRecv()
		if receiptErr != nil {
			return receiptErr
		}
		if !result.GetAvailable() {
			return errors.New("Guest did not acknowledge staged content")
		}
		return nil
	}
	if err != nil {
		return err
	}
	buffer := make([]byte, vmmdv1.MaxImportArchiveChunkBytes)
	for {
		count, readErr := archive.Read(buffer)
		if count > 0 {
			sent += uint64(count)
			err = stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_ArchiveChunk{ArchiveChunk: append([]byte(nil), buffer[:count]...)}})
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
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
	result, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if !result.GetAvailable() {
		return errors.New("Guest did not acknowledge staged content")
	}
	return nil
}
