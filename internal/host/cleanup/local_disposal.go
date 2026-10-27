//go:build darwin || linux

package cleanup

import (
	"context"
	"database/sql"
	"errors"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Owner) PlanLocalDisposal(ctx context.Context, request *hostv1.PlanLocalDisposalRequest) (*hostv1.PlanLocalDisposalResponse, error) {
	if request.GetTargetId() == "" {
		return nil, hostv1.InvalidField("target_id", "an exact local target is required")
	}
	plan := &hostv1.LocalDisposalPlan{TargetKind: request.GetTargetKind(), TargetId: request.GetTargetId(), ActivityUnknown: true}
	switch request.GetTargetKind() {
	case hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT:
		environment, err := server.store.GetEnvironment(ctx, request.GetTargetId())
		if err != nil {
			return nil, ReadError(err)
		}
		if environment.OwnerKind != store.OwnerLocal {
			return nil, status.Error(codes.FailedPrecondition, "remote environments require a new plan for their entire registration")
		}
		plan.NamespaceId, plan.Revision, plan.DisplayName, plan.EnvironmentCount = environment.NamespaceID, environment.PolicyRevision, environment.DisplayName, 1
	case hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION:
		registration, err := server.store.GetRemoteRegistration(ctx, request.GetTargetId())
		if err != nil {
			return nil, ReadError(err)
		}
		if registration.OwnerKind != store.RemoteRegistrationExternal {
			return nil, status.Error(codes.FailedPrecondition, "private system installations use their own removal owner")
		}
		plan.Revision, plan.DisplayName = registration.Revision, registration.DisplayName
		count, err := server.store.RemoteEnvironmentCount(ctx, registration.ID)
		if err != nil {
			return nil, ReadError(err)
		}
		plan.EnvironmentCount = count
	default:
		return nil, hostv1.InvalidField("target_kind", "select a local environment or exact external registration")
	}
	return &hostv1.PlanLocalDisposalResponse{Plan: plan}, nil
}

func ReadError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return status.Error(codes.NotFound, "local target does not exist")
	}
	return status.Error(codes.Unavailable, "local target mapping could not be read")
}

func (server *Owner) ExecuteLocalDisposal(ctx context.Context, request *hostv1.ExecuteLocalDisposalRequest) (*hostv1.ExecuteLocalDisposalResponse, error) {
	plan := request.GetPlan()
	if request.GetRequestId() == "" || len(request.GetRequestId()) > 128 || plan == nil || plan.GetRevision() == 0 || !request.GetIrreversibleConfirmation() {
		return nil, hostv1.InvalidField("request_id/plan/irreversible_confirmation", "confirm the exact revision-bound local deletion plan")
	}
	kind := "environment"
	if plan.GetTargetKind() == hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION {
		kind = "registration"
	} else if plan.GetTargetKind() != hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT {
		return nil, hostv1.InvalidField("target_kind", "invalid local deletion scope")
	}
	releaseInventory, err := server.inventory.Transition(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	unlock := func() {}
	if kind == "environment" {
		unlock, err = server.workloads.LockEnvironment(ctx, plan.GetTargetId())
		if err != nil {
			return nil, err
		}
	}
	defer unlock()
	if err := server.services.ProjectionLock().Lock(ctx); err != nil {
		return nil, err
	}

	defer server.services.ProjectionLock().Unlock()
	releaseAdmission := server.remote.LockAdmission()
	defer releaseAdmission()
	id, err := opreceipt.NewID()
	if err != nil {
		return nil, err
	}
	operation, err := server.store.AcceptLocalDisposal(ctx, store.Operation{ID: id, RequestID: request.GetRequestId()}, store.LocalDisposal{Kind: kind, TargetID: plan.GetTargetId(), NamespaceID: plan.GetNamespaceId(), Revision: plan.GetRevision()})
	if err != nil {
		return nil, status.Error(codes.Aborted, "local deletion scope changed or its durable receipt could not be written")
	}
	if kind == "registration" {
		server.remote.StopExternalConnector(plan.GetTargetId())
	}
	server.WakeLocalDisposal()
	return &hostv1.ExecuteLocalDisposalResponse{Operation: opreceipt.FromStore(operation)}, nil
}

func (controller *Owner) WakeLocalDisposal() {
	select {
	case controller.localDisposalWake <- struct{}{}:
	default:
	}
}

// One shared observer visits at most 32 roots per page. Each root gets one bounded Guest batch.
// The cursor advances on failure so an unavailable target does not starve other scopes.
func (controller *Owner) LocalDisposalLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	cursor := ""
	for {
		page, err := controller.store.PendingLocalDisposals(ctx, cursor, 32)
		if err == nil {
			if len(page) == 0 {
				cursor = ""
			}
			for _, root := range page {
				if ctx.Err() != nil {
					return
				}
				cursor = root.TargetID
				_ = controller.ReconcileLocalDisposal(ctx, root)
			}
			if len(page) < 32 {
				cursor = ""
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-controller.localDisposalWake:
		}
	}
}

func (controller *Owner) ReconcileLocalDisposal(ctx context.Context, root store.LocalDisposal) (failure error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	operation, err := controller.store.GetOperation(ctx, root.OperationID)
	if err != nil {
		return err
	}
	defer func() {
		if failure != nil {
			operation.ErrorReason = "LOCAL_DISPOSAL_PENDING"
			operation.ErrorMessage = "The target is isolated. Local deletion must continue when its runtime is available."
			saved, cancelSave := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelSave()
			_ = controller.store.UpdateOperation(saved, operation)
		}
	}()
	var environment store.Environment
	if root.Kind == "registration" {
		controller.remote.StopExternalConnector(root.TargetID)
		if err := controller.remote.DeleteCredential(ctx, root.TargetID); err != nil {
			return err
		}
		page, err := controller.store.ListRemoteEnvironmentPage(ctx, root.TargetID, "", 1)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return controller.store.FinishLocalDisposal(ctx, root)
		}
		environment = page[0]
	} else {
		environment, err = controller.store.GetEnvironment(ctx, root.TargetID)
		if errors.Is(err, sql.ErrNoRows) {
			environment = store.Environment{ID: root.TargetID, NamespaceID: root.NamespaceID, OwnerKind: store.OwnerLocal}
		} else if err != nil {
			return err
		}
	}
	child, err := controller.store.CaptureDisposalEnvironment(ctx, root, environment)
	if err != nil {
		return err
	}
	shareErr := controller.hostShares.SettleEnvironmentHostShares(ctx, child.TargetID)
	unlock, lockErr := controller.workloads.LockEnvironment(ctx, child.TargetID)
	if lockErr != nil {
		return lockErr
	}
	defer unlock()
	sessions, err := controller.store.DisposalSessionPage(ctx, child.TargetID)
	if err != nil {
		return err
	}
	for _, id := range sessions {
		controller.sessions.CloseDisposed(id)
		if err := controller.store.CloseDisposedSession(ctx, child.TargetID, id); err != nil {
			return err
		}
	}
	if shareErr != nil {
		return shareErr
	}
	if len(sessions) > 0 {
		controller.WakeLocalDisposal()
		return nil
	}
	if err := controller.store.RevokeGrants(ctx, child.NamespaceID); err != nil {
		return err
	}
	client, err := controller.guestConnection.Client()
	if err != nil {
		return err
	}

	imports, err := controller.store.DisposalServicePage(ctx, child.TargetID)
	if err != nil {
		return err
	}
	for _, id := range imports {
		if controller.egress != nil {
			controller.egress.DeleteServiceRoute(id)
		}
		if _, err := client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-service-" + id), ProjectionId: id}); err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		if err := controller.store.FinishDisposalService(ctx, child.TargetID, id); err != nil {
			return err
		}
	}
	if len(imports) > 0 {
		controller.WakeLocalDisposal()
		return nil
	}
	grants, err := controller.store.GrantsForNamespace(ctx, child.NamespaceID, 31)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Kind == "network-link" {
			_, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-link-" + grant.ID), LinkId: grant.ID})
			if err != nil && status.Code(err) != codes.NotFound {
				return err
			}
		}
		if err := controller.store.CompactRevokedGrant(ctx, grant.ID); err != nil {
			return err
		}
	}
	if len(grants) > 0 {
		return nil
	}
	result, err := client.DisposeNamespaceBatch(ctx, &vmmdv1.DisposeNamespaceBatchRequest{RequestId: opreceipt.DerivedRequestID(operation.RequestID + "-namespace-" + child.NamespaceID), NamespaceId: child.NamespaceID})
	if err != nil {
		return err
	}
	operation.Stage = result.GetStage()
	operation.ErrorReason, operation.ErrorMessage = "", ""
	if err := controller.store.UpdateOperation(ctx, operation); err != nil {
		return err
	}
	if !result.GetComplete() {
		controller.WakeLocalDisposal()
		return nil
	}
	if err := controller.store.FinishDisposalEnvironment(ctx, child); err != nil {
		return err
	}
	controller.workloads.ForgetContainers(child.TargetID)
	if root.Kind == "environment" {
		return controller.store.FinishLocalDisposal(ctx, root)
	}
	controller.WakeLocalDisposal()
	return nil
}

func (controller *Owner) RequireUndisposedEnvironment(ctx context.Context, id string) error {
	environment, err := controller.store.GetEnvironment(ctx, id)
	if err != nil {
		return ReadError(err)
	}
	disposed, err := controller.store.EnvironmentDisposed(ctx, environment)
	if err != nil {
		return ReadError(err)
	}
	if disposed {
		return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_LOCAL_DISPOSED, "the local operator disposed this environment", hostv1.WithResource("environment", id))
	}
	return nil
}

func (controller *Owner) RequireUndisposedRegistration(ctx context.Context, id, enrollmentRequestID string) error {
	disposed, err := controller.store.RegistrationDisposed(ctx, id, enrollmentRequestID)
	if err != nil {
		return status.Error(codes.Unavailable, "local revocation facts could not be read")
	}
	if disposed {
		return hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_LOCAL_DISPOSED, "the local operator disposed this registration", hostv1.WithResource("remote_registration", id))
	}
	return nil
}
