//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/rand"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/mcptoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (owner *Owner) CreateMCPToken(ctx context.Context) (string, error) {
	unlock, err := owner.applianceLocks.Lock(ctx, mcptoken.InstallationID)
	if err != nil {
		return "", err
	}
	defer unlock()
	installation, err := owner.store.GetApplianceInstallation(ctx, mcptoken.InstallationID)
	if err != nil {
		return "", err
	}
	if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) {
		return "", status.Error(codes.FailedPrecondition, "MCP appliance is not running")
	}
	key, err := owner.applianceSecrets.LoadMCPTokenKey(ctx, mcptoken.InstallationID)
	if err != nil {
		return "", err
	}
	defer clear(key)
	return mcptoken.Create(key)
}

func (owner *Owner) RotateMCPTokenKey(ctx context.Context) (*appliancev1.ApplianceOperation, error) {
	observed, err := owner.GetApplianceStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: mcptoken.InstallationID})
	if err != nil {
		return nil, err
	}
	requestID, err := operation.NewID()
	if err != nil {
		return nil, err
	}
	seed := make([]byte, mcptoken.KeySize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	defer clear(seed)
	current := observed.GetStatus()
	updated, err := owner.UpdateAppliance(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: mcptoken.InstallationID,
		RequestId: requestID, ExpectedRevision: current.GetRevision(), ArtifactId: current.GetArtifactId(),
		ApprovedGrant: current.GetApprovedGrant(), DeadlineUnixMillis: time.Now().Add(10 * time.Minute).UnixMilli(), OpaqueBootstrapSeed: seed})
	if err != nil {
		return nil, err
	}
	return updated.GetOperation(), nil
}
