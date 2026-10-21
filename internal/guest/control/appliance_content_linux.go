//go:build linux

package control

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
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func receiveContentRequest(ctx context.Context, recv func() (*appliancev1.StageContentRequest, error)) (*appliancev1.StageContentRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		request *appliancev1.StageContentRequest
		err     error
	}
	ready := make(chan result, 1)
	go func() { request, err := recv(); ready <- result{request, err} }()
	timer := time.NewTimer(30 * time.Second)
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

func (s *Server) StageApplianceContent(stream grpc.ClientStreamingServer[appliancev1.StageContentRequest, appliancev1.ApplianceContentStatus]) error {
	first, err := receiveContentRequest(stream.Context(), stream.Recv)
	if err != nil {
		return rpcError(err)
	}
	header := first.GetHeader()
	if header == nil || proto.Size(header) > 32<<10 || header.GetRequestId() == "" || header.GetDeadlineUnixMillis() <= time.Now().UnixMilli() {
		return invalid(errors.New("bounded content stage header is required"))
	}
	content := header.GetContentRef()
	if err := appliancecontent.ValidateReference(content); err != nil {
		return invalid(err)
	}
	ctx, cancel := context.WithDeadline(stream.Context(), time.UnixMilli(header.GetDeadlineUnixMillis()))
	defer cancel()
	root := filepath.Join(stateRoot, "appliance-content")
	available, err := appliancecontent.Available(root, content)
	if err != nil {
		return rpcError(err)
	}
	if available {
		log.Printf("appliance content Guest cache ready: role=%s digest=%s archive_bytes=%d", content.GetRole(), content.GetImageManifestDigest(), content.GetArchiveSize())
		return stream.SendAndClose(&appliancev1.ApplianceContentStatus{ContentRef: content, Available: true})
	}
	unlock, err := s.runtime.lockImportSlot(ctx, content.GetImageManifestDigest())
	if err != nil {
		return rpcError(err)
	}
	defer unlock()
	available, err = appliancecontent.Available(root, content)
	if err != nil {
		return rpcError(err)
	}
	if available {
		log.Printf("appliance content Guest cache ready: role=%s digest=%s archive_bytes=%d", content.GetRole(), content.GetImageManifestDigest(), content.GetArchiveSize())
		return stream.SendAndClose(&appliancev1.ApplianceContentStatus{ContentRef: content, Available: true})
	}
	importHeader := &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ManifestDigest: content.GetImageManifestDigest(), ArchiveSize: content.GetArchiveSize()}
	reservation, err := s.runtime.reserveImport(ctx, importHeader)
	if err != nil {
		return rpcError(err)
	}
	defer reservation.Release()
	var archive *os.File
	err = s.runtime.diskBudget.Mutate(func(_ *guestdisk.Transaction) error {
		if err := s.runtime.openOCIProjectAllowance(reservation); err != nil {
			return err
		}
		if err := ensureDirectory(root, 0700); err != nil {
			return err
		}
		if err := assignOCIRoot(root, projectID, assignProject); err != nil {
			return err
		}
		var err error
		archive, err = os.CreateTemp(root, ".content-stage-*")
		return err
	})
	if err != nil {
		return rpcError(err)
	}
	defer archive.Close()
	defer os.Remove(archive.Name())
	defer s.runtime.diskBudget.Settle(func() error { return s.runtime.settleOCIProjectQuota(context.Background()) })
	written := uint64(0)
	for {
		frame, err := receiveContentRequest(ctx, stream.Recv)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return rpcError(err)
		}
		chunk := frame.GetArchiveChunk()
		if !validImportArchiveChunk(chunk, content.GetArchiveSize()-written) {
			return invalid(errors.New("archive chunk exceeds content limits"))
		}
		count, err := writeImportChunk(s.runtime.diskBudget, reservation, archive, chunk)
		if err != nil {
			return rpcError(err)
		}
		written += uint64(count)
	}
	if written != content.GetArchiveSize() {
		return invalid(errors.New("content archive is truncated"))
	}
	if err := appliancecontent.ValidateArchive(ctx, archive, content); err != nil {
		return invalid(err)
	}
	err = s.runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return s.runtime.guardOCIProjectGrowth(transaction, reservation, func() error { return appliancecontent.Publish(root, archive, content) })
	})
	if err != nil {
		return rpcError(err)
	}
	log.Printf("appliance content Guest stage ready: role=%s digest=%s archive_bytes=%d", content.GetRole(), content.GetImageManifestDigest(), written)
	return stream.SendAndClose(&appliancev1.ApplianceContentStatus{ContentRef: content, Available: true})
}

func (r *Runtime) projectApplianceContent(ctx context.Context, request *vmmdv1.EnsureSystemApplianceRequest) (string, error) {
	root := filepath.Join(stateRoot, "appliance-content-projections", request.GetIdentity().GetInstallationId())
	if err := os.RemoveAll(root); err != nil {
		return "", err
	}
	if err := ensureDirectory(root, 0755); err != nil {
		return "", err
	}
	if err := assignOCIRoot(root, projectID, assignProject); err != nil {
		return "", err
	}
	for _, content := range request.GetContentRefs() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := appliancecontent.ValidateReference(content); err != nil {
			return "", err
		}
		if content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER {
			continue
		}
		sourceRoot := filepath.Join(stateRoot, "appliance-content")
		available, err := appliancecontent.Available(sourceRoot, content)
		if err != nil {
			return "", err
		}
		if !available {
			return "", errors.New("approved appliance content is missing")
		}
		name := appliancecontent.ArchiveName(content)
		destination := filepath.Join(root, name)
		if _, err := os.Lstat(destination); err == nil {
			continue
		}
		if err := os.Link(filepath.Join(sourceRoot, name), destination); err != nil {
			return "", err
		}
	}
	return root, nil
}
