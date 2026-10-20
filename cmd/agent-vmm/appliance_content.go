//go:build darwin

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
)

func stagePackageContent(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, directory string, manifest *appliancev1.ControllerApplianceManifest, deadline int64) error {
	statuses, err := client.GetContentStatus(ctx, &appliancev1.GetContentStatusRequest{ContentRefs: manifest.GetContentRefs()})
	if err != nil {
		return err
	}
	for _, contentStatus := range statuses.GetContents() {
		content := contentStatus.GetContentRef()
		fmt.Printf("content role=%s digest=%s bytes=%d ", content.GetRole(), content.GetImageManifestDigest(), content.GetArchiveSize())
		if contentStatus.GetAvailable() {
			fmt.Println("cached")
			continue
		}
		fmt.Println("staging")
		name := filepath.Join(directory, "content", appliancecontent.ArchiveName(content))
		if content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER {
			name = filepath.Join(directory, "image.oci.tar")
		}
		if err := stagePackageArchive(ctx, client, name, content, deadline); err != nil {
			return err
		}
	}
	return nil
}

func stagePackageArchive(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, path string, content *appliancev1.ApplianceContentReference, deadline int64) error {
	archive, err := os.Open(path)
	if err != nil {
		return err
	}
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil {
		return err
	}
	if uint64(info.Size()) != content.GetArchiveSize() {
		return errors.New("package archive size does not match the manifest")
	}
	stream, err := client.StageContent(ctx)
	if err != nil {
		return err
	}
	err = stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_Header{Header: &appliancev1.StageContentHeader{RequestId: lifecycleRequestID(), DeadlineUnixMillis: deadline, ContentRef: content}}})
	if err != nil {
		return err
	}
	buffer := make([]byte, 1<<20)
	var sent, nextReport uint64
	nextReport = 256 << 20
	for {
		count, readErr := archive.Read(buffer)
		if count > 0 {
			err = stream.Send(&appliancev1.StageContentRequest{Payload: &appliancev1.StageContentRequest_ArchiveChunk{ArchiveChunk: append([]byte(nil), buffer[:count]...)}})
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			sent += uint64(count)
			if sent >= nextReport {
				fmt.Printf("content sent_bytes=%d total_bytes=%d\n", sent, content.GetArchiveSize())
				nextReport = sent + (256 << 20)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	receipt, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	fmt.Printf("content receipt=%s state=%s\n", receipt.GetOperation().GetOperationId(), receipt.GetOperation().GetState())
	if receipt.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED {
		return errors.New("content stage did not succeed")
	}
	return nil
}
