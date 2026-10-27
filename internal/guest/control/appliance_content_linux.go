//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func receiveContentRequest(ctx context.Context, recv func() (*vmmdv1.StageApplianceContentRequest, error)) (*vmmdv1.StageApplianceContentRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		request *vmmdv1.StageApplianceContentRequest
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

func (s *Server) StageApplianceContent(stream grpc.ClientStreamingServer[vmmdv1.StageApplianceContentRequest, appliancev1.ApplianceContentStatus]) error {
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
		s.runtime.logger.Info("appliance content cache ready", "role", content.GetRole(), "digest", content.GetImageManifestDigest(), "archive_bytes", content.GetArchiveSize())
		return stream.SendAndClose(&appliancev1.ApplianceContentStatus{ContentRef: content, Available: true})
	}
	unlock, err := s.runtime.lockImportSlot(ctx)
	if err != nil {
		return rpcError(err)
	}
	defer unlock()
	available, err = appliancecontent.Available(root, content)
	if err != nil {
		return rpcError(err)
	}
	if available {
		s.runtime.logger.Info("appliance content cache ready", "role", content.GetRole(), "digest", content.GetImageManifestDigest(), "archive_bytes", content.GetArchiveSize())
		return stream.SendAndClose(&appliancev1.ApplianceContentStatus{ContentRef: content, Available: true})
	}
	importHeader := &vmmdv1.ImportImageHeader{RequestId: header.GetRequestId(), ManifestDigest: content.GetImageManifestDigest(), ArchiveSize: content.GetArchiveSize()}
	reservation, err := s.runtime.reserveImport(ctx, importHeader)
	if err != nil {
		return rpcError(err)
	}
	defer reservation.Release()
	archive, cleanup, err := s.runtime.receiveArchive(ctx, func() ([]byte, error) {
		frame, err := receiveContentRequest(ctx, stream.Recv)
		if err != nil {
			return nil, err
		}
		return frame.GetArchiveChunk(), nil
	}, content.GetArchiveSize(), content.GetArchiveSha256(), reservation)
	if err != nil {
		return rpcError(err)
	}
	defer cleanup()
	if _, err := ociarchive.Validate(archive, digest.Digest(content.GetImageManifestDigest())); err != nil {
		return invalid(err)
	}
	err = s.runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return s.runtime.guardOCIProjectGrowth(transaction, reservation, func() error { return appliancecontent.Publish(root, archive, content) })
	})
	if err != nil {
		return rpcError(err)
	}
	s.runtime.logger.Info("appliance content stage ready", "role", content.GetRole(), "digest", content.GetImageManifestDigest(), "archive_bytes", content.GetArchiveSize())
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
