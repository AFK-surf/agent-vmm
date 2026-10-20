//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) applianceContentRoot() string {
	return filepath.Join(controller.supportDirectory, "appliance-artifacts")
}

func (server *applianceAdminServer) GetContentStatus(ctx context.Context, request *appliancev1.GetContentStatusRequest) (*appliancev1.GetContentStatusResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if proto.Size(request) > 32<<10 {
		return nil, status.Error(codes.ResourceExhausted, "content references exceed the request limit")
	}
	response := new(appliancev1.GetContentStatusResponse)
	for _, content := range request.GetContentRefs() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := appliancecontent.ValidateReference(content); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		available, err := appliancecontent.Available(server.controller.applianceContentRoot(), content)
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		response.Contents = append(response.Contents, &appliancev1.ApplianceContentStatus{ContentRef: content, Available: available})
	}
	return response, nil
}

func (server *applianceAdminServer) StageContent(stream grpc.ClientStreamingServer[appliancev1.StageContentRequest, appliancev1.ApplianceOperationResponse]) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	first, err := receiveImportRequest(stream.Context(), stream.Recv, importReceiveIdleTimeout)
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil || proto.Size(header) > 32<<10 || header.GetRequestId() == "" {
		return status.Error(codes.InvalidArgument, "a bounded content stage header is required")
	}
	if err := appliancecontent.ValidateReference(header.GetContentRef()); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	lookup := func(ctx context.Context, settle bool) (bool, error) {
		existing, err := server.controller.store.ApplianceOperationByRequest(ctx, header.GetRequestId())
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return true, err
		}
		prior := new(appliancev1.StageContentHeader)
		if existing.Kind != int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE_CONTENT) || proto.Unmarshal(existing.Receipt, prior) != nil || !proto.Equal(prior, header) {
			return true, status.Error(codes.AlreadyExists, "content request ID has a different payload")
		}
		if existing.State == int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) {
			if !settle {
				return false, nil
			}
			completionCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			available, err := appliancecontent.Available(server.controller.applianceContentRoot(), header.GetContentRef())
			if err != nil {
				return true, err
			}
			if available {
				existing, err = server.completeApplianceOperation(completionCtx, existing.ID, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "content-staged", existing.Receipt)
			} else {
				existing, err = server.controller.store.TransitionApplianceOperation(completionCtx, existing.ID, store.ApplianceOperationTransition{ExpectedState: 1, ExpectedPhase: 1, State: 3, Phase: 5, TerminalReason: 8, Stage: "content-stage-failed", ErrorReason: "CONTENT_STAGE_INTERRUPTED", ErrorMessage: "content stage ended before publication; retry with a new stage request", Receipt: existing.Receipt})
			}
			if err != nil {
				return true, err
			}
		}
		return true, stream.SendAndClose(applianceOperationResponse(existing))
	}
	if found, err := lookup(stream.Context(), false); found || err != nil {
		return err
	}
	lock := server.controller.applianceContentStageLock()
	if header.GetDeadlineUnixMillis() <= time.Now().UnixMilli() {
		if !lock.TryLock() {
			return status.Error(codes.DeadlineExceeded, "content stage deadline elapsed while admission is in progress")
		}
		defer lock.Unlock()
		if found, err := lookup(stream.Context(), true); found || err != nil {
			return err
		}
		return resourceErrorWithMetadata(codes.DeadlineExceeded, "CONTENT_STAGE_NOT_ADMITTED", "appliance_content", header.GetRequestId(), "content stage was not admitted before its deadline", map[string]string{"admission": "not_accepted"})
	}
	ctx, cancel := context.WithDeadline(stream.Context(), time.UnixMilli(header.GetDeadlineUnixMillis()))
	defer cancel()
	for !lock.TryLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return err
		}
	}
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	receipt, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	if found, err := lookup(ctx, true); found || err != nil {
		return err
	}
	operationID, err := randomID()
	if err != nil {
		return err
	}
	operation, err := server.controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: operationID, RequestID: header.GetRequestId(), Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE_CONTENT), State: 1, Phase: 1, DeadlineUnixMillis: header.GetDeadlineUnixMillis(), Stage: "content-validating", Receipt: receipt})
	if err != nil {
		return err
	}
	stageErr := server.receiveApplianceContent(ctx, stream, header.GetContentRef())
	completionCtx, completionCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer completionCancel()
	if stageErr != nil {
		_, _ = server.controller.store.TransitionApplianceOperation(completionCtx, operation.ID, store.ApplianceOperationTransition{ExpectedState: 1, ExpectedPhase: 1, State: 3, Phase: 5, TerminalReason: 8, Stage: "content-stage-failed", ErrorReason: "CONTENT_STAGE_FAILED", ErrorMessage: stageErr.Error(), Receipt: receipt})
		return stageErr
	}
	operation, err = server.completeApplianceOperation(completionCtx, operation.ID, appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED, "content-staged", receipt)
	if err != nil {
		return err
	}
	return stream.SendAndClose(applianceOperationResponse(operation))
}

func (server *applianceAdminServer) receiveApplianceContent(ctx context.Context, stream grpc.ClientStreamingServer[appliancev1.StageContentRequest, appliancev1.ApplianceOperationResponse], content *appliancev1.ApplianceContentReference) error {
	root := server.controller.applianceContentRoot()
	available, err := appliancecontent.Available(root, content)
	if err != nil || available {
		return err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	var capacity unix.Statfs_t
	if err := unix.Statfs(root, &capacity); err != nil {
		return err
	}
	// Preserve host filesystem headroom for control state and other product files.
	for uint64(capacity.Bavail)*uint64(capacity.Bsize) < content.GetArchiveSize()+(2<<30) {
		removed, err := server.controller.collectApplianceContent(ctx)
		if err != nil {
			return err
		}
		if err := unix.Statfs(root, &capacity); err != nil {
			return err
		}
		if removed == 0 && uint64(capacity.Bavail)*uint64(capacity.Bsize) < content.GetArchiveSize()+(2<<30) {
			return status.Error(codes.ResourceExhausted, "content stage requires archive capacity and filesystem headroom")
		}
	}
	file, err := os.CreateTemp(root, ".content-stage-*")
	if err != nil {
		return err
	}
	defer file.Close()
	defer os.Remove(file.Name())
	written := uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame, err := receiveImportRequest(ctx, stream.Recv, importReceiveIdleTimeout)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk := frame.GetArchiveChunk()
		if len(chunk) == 0 || len(chunk) > applianceArchiveChunkBytes || uint64(len(chunk)) > content.GetArchiveSize()-written {
			return status.Error(codes.InvalidArgument, "content chunks must fit frame and archive limits")
		}
		count, err := file.Write(chunk)
		if err != nil {
			return err
		}
		if count != len(chunk) {
			return io.ErrShortWrite
		}
		written += uint64(count)
	}
	if written != content.GetArchiveSize() {
		return status.Error(codes.InvalidArgument, "content archive is truncated")
	}
	if err := appliancecontent.ValidateArchive(ctx, file, content); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return appliancecontent.Publish(root, file, content)
}
