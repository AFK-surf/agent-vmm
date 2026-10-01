//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *operatorServer) PlanLocalDisposal(ctx context.Context, request *hostv1.PlanLocalDisposalRequest) (*hostv1.LocalDisposalPlanResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetTargetId() == "" {
		return nil, invalidField("target_id", "an exact local target is required")
	}
	plan := &hostv1.LocalDisposalPlan{TargetKind: request.GetTargetKind(), TargetId: request.GetTargetId(), ActivityUnknown: true}
	switch request.GetTargetKind() {
	case hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT:
		environment, err := server.controller.store.GetEnvironment(ctx, request.GetTargetId())
		if err != nil {
			return nil, disposalReadError(err, request.GetTargetId())
		}
		if environment.OwnerKind != store.OwnerLocal {
			return nil, resourceError(codes.FailedPrecondition, "REGISTRATION_SCOPE_REQUIRED", "environment", environment.ID, "remote environments require a new plan for their entire registration")
		}
		plan.NamespaceId, plan.Revision, plan.DisplayName, plan.EnvironmentCount = environment.NamespaceID, environment.PolicyRevision, environment.DisplayName, 1
	case hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION:
		registration, err := server.controller.store.GetRemoteRegistration(ctx, request.GetTargetId())
		if err != nil {
			return nil, disposalReadError(err, request.GetTargetId())
		}
		if registration.OwnerKind != store.RemoteRegistrationExternal {
			return nil, resourceError(codes.FailedPrecondition, "PRIVATE_INSTALLATION_OWNER_REQUIRED", "remote_registration", registration.ID, "private system installations use their own removal owner")
		}
		plan.Revision, plan.DisplayName = registration.Revision, registration.DisplayName
		count, err := server.controller.store.RemoteEnvironmentCount(ctx, registration.ID)
		if err != nil {
			return nil, disposalReadError(err, registration.ID)
		}
		plan.EnvironmentCount = count
	default:
		return nil, invalidField("target_kind", "select a local environment or exact external registration")
	}
	return &hostv1.LocalDisposalPlanResponse{Plan: plan}, nil
}

func disposalReadError(err error, id string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return resourceError(codes.NotFound, "LOCAL_TARGET_ABSENT", "local_target", id, "local target does not exist")
	}
	return resourceError(codes.Unavailable, "LOCAL_TARGET_UNREADABLE", "local_target", id, "local target mapping could not be read")
}

func (server *operatorServer) ExecuteLocalDisposal(ctx context.Context, request *hostv1.ExecuteLocalDisposalRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	plan := request.GetPlan()
	if request.GetRequestId() == "" || len(request.GetRequestId()) > 128 || plan == nil || plan.GetRevision() == 0 || !request.GetIrreversibleConfirmation() {
		return nil, invalidField("request_id/plan/irreversible_confirmation", "confirm the exact revision-bound local deletion plan")
	}
	kind := "environment"
	if plan.GetTargetKind() == hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_REGISTRATION {
		kind = "registration"
	} else if plan.GetTargetKind() != hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT {
		return nil, invalidField("target_kind", "invalid local deletion scope")
	}
	releaseInventory, err := server.controller.lockInventoryTransition(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseInventory()
	releaseAdmission, err := server.controller.lockRemoteAdmission(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseAdmission()
	unlock := func() {}
	if kind == "environment" {
		unlock, err = server.controller.lockEnvironmentContext(ctx, plan.GetTargetId())
		if err != nil {
			return nil, err
		}
	}
	defer unlock()
	server.controller.projectionMu.Lock()
	defer server.controller.projectionMu.Unlock()
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	operation, err := server.controller.store.AcceptLocalDisposal(ctx, store.Operation{ID: id, RequestID: request.GetRequestId()}, store.LocalDisposal{Kind: kind, TargetID: plan.GetTargetId(), NamespaceID: plan.GetNamespaceId(), Revision: plan.GetRevision()})
	if err != nil {
		return nil, resourceError(codes.Aborted, "LOCAL_DISPOSAL_CONFLICT", "local_target", plan.GetTargetId(), "local deletion scope changed or its durable receipt could not be written")
	}
	if kind == "registration" {
		server.controller.stopExternalConnector(plan.GetTargetId())
	}
	server.controller.wakeLocalDisposal()
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (controller *Controller) wakeLocalDisposal() {
	select {
	case controller.localDisposalWake <- struct{}{}:
	default:
	}
}

// One shared observer visits at most 32 roots per page. Each root gets one bounded Guest batch.
// The cursor advances on failure so an unavailable target does not starve other scopes.
func (controller *Controller) localDisposalLoop(ctx context.Context) {
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
				_ = controller.reconcileLocalDisposal(ctx, root)
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

func (controller *Controller) reconcileLocalDisposal(ctx context.Context, root store.LocalDisposal) (failure error) {
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
		controller.stopExternalConnector(root.TargetID)
		if controller.remoteCredentials != nil {
			if err := controller.remoteCredentials.Delete(ctx, root.TargetID); err != nil {
				return err
			}
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
	unlock, err := controller.lockEnvironmentContext(ctx, child.TargetID)
	if err != nil {
		return err
	}
	defer unlock()
	sessions, err := controller.store.DisposalSessionPage(ctx, child.TargetID)
	if err != nil {
		return err
	}
	for _, id := range sessions {
		controller.mu.Lock()
		session := controller.sessions[id]
		delete(controller.sessions, id)
		controller.mu.Unlock()
		if session != nil {
			session.server.Stop()
			_ = session.listener.Close()
			_ = os.Remove(session.path)
		}
		if err := controller.store.CloseDisposedSession(ctx, child.TargetID, id); err != nil {
			return err
		}
	}
	if len(sessions) > 0 {
		controller.wakeLocalDisposal()
		return nil
	}
	if err := controller.store.RevokeGrants(ctx, child.NamespaceID); err != nil {
		return err
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	imports, err := controller.store.DisposalServicePage(ctx, child.TargetID)
	if err != nil {
		return err
	}
	for _, id := range imports {
		if controller.egress != nil {
			controller.egress.DeleteServiceRoute(id)
		}
		if _, err := client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: derivedRequestID(operation.RequestID + "-service-" + id), ProjectionId: id}); err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		if err := controller.store.FinishDisposalService(ctx, child.TargetID, id); err != nil {
			return err
		}
	}
	if len(imports) > 0 {
		controller.wakeLocalDisposal()
		return nil
	}
	grants, err := controller.store.GrantsForNamespace(ctx, child.NamespaceID, 31)
	if err != nil {
		return err
	}
	for _, grant := range grants {
		if grant.Kind == "network-link" {
			_, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: derivedRequestID(operation.RequestID + "-link-" + grant.ID), LinkId: grant.ID})
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
	result, err := client.DisposeNamespaceBatch(ctx, &vmmdv1.DisposeNamespaceBatchRequest{RequestId: derivedRequestID(operation.RequestID + "-namespace-" + child.NamespaceID), NamespaceId: child.NamespaceID})
	if err != nil {
		return err
	}
	operation.Stage = result.GetStage()
	operation.ErrorReason, operation.ErrorMessage = "", ""
	if err := controller.store.UpdateOperation(ctx, operation); err != nil {
		return err
	}
	if !result.GetComplete() {
		controller.wakeLocalDisposal()
		return nil
	}
	if err := controller.store.FinishDisposalEnvironment(ctx, child); err != nil {
		return err
	}
	controller.forgetContainers(child.TargetID)
	if root.Kind == "environment" {
		return controller.store.FinishLocalDisposal(ctx, root)
	}
	controller.wakeLocalDisposal()
	return nil
}

func (controller *Controller) requireUndisposedEnvironment(ctx context.Context, id string) error {
	environment, err := controller.store.GetEnvironment(ctx, id)
	if err != nil {
		return disposalReadError(err, id)
	}
	disposed, err := controller.store.EnvironmentDisposed(ctx, environment)
	if err != nil {
		return disposalReadError(err, id)
	}
	if disposed {
		return resourceError(codes.FailedPrecondition, "LOCAL_DISPOSED", "environment", id, "the local operator disposed this environment")
	}
	return nil
}

func (controller *Controller) requireUndisposedRegistration(ctx context.Context, id, enrollmentRequestID string) error {
	disposed, err := controller.store.RegistrationDisposed(ctx, id, enrollmentRequestID)
	if err != nil {
		return resourceError(codes.Unavailable, "LOCAL_TARGET_UNREADABLE", "remote_registration", id, "local revocation facts could not be read")
	}
	if disposed {
		return resourceError(codes.FailedPrecondition, "LOCAL_DISPOSED", "remote_registration", id, "the local operator disposed this registration")
	}
	return nil
}
