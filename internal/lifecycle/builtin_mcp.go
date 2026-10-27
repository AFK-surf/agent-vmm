//go:build darwin || linux

package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const builtinMCPInstallation = "agent-vmm-mcp"

func reconcileBuiltinMCP(value paths) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := installBuiltinMCP(ctx, value); err != nil {
		slog.Warn("MCP appliance is unavailable", "error", err)
	}
}

func installBuiltinMCP(ctx context.Context, value paths) error {
	encoded, err := os.ReadFile(filepath.Join(builtinMCPPackageDirectory(value), "manifest.pb"))
	if err != nil {
		return err
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(encoded, manifest); err != nil {
		return err
	}
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return err
	}
	defer connection.Close()
	return applyBuiltinMCP(ctx, appliancev1.NewApplianceAdminServiceClient(connection), manifest)
}

func applyBuiltinMCP(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, manifest *appliancev1.ControllerApplianceManifest) error {
	deadline := time.Now().Add(10 * time.Minute).UnixMilli()
	staged, err := client.StageArtifact(ctx, &appliancev1.StageArtifactRequest{RequestId: newOperationID("mcp-stage"), DeadlineUnixMillis: deadline, Manifest: manifest})
	if err != nil {
		return err
	}
	observed, err := client.GetApplianceStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: builtinMCPInstallation})
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	current := observed.GetStatus()
	if current != nil && current.GetArtifactId() == staged.GetArtifactId() {
		return nil
	}
	grant := &appliancev1.ApprovedGrant{SystemLimits: manifest.GetSystemLimits(), Capacity: manifest.GetRequestedCapacity(), EgressMode: manifest.GetRequestedEgressMode(), Frontends: manifest.GetRequestedFrontends(), LanEgress: manifest.GetRequestedLanEgress()}
	requestID := newOperationID("mcp-install")
	if current != nil {
		updated, err := client.UpdateAppliance(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: builtinMCPInstallation, RequestId: requestID, ExpectedRevision: current.GetRevision(), ArtifactId: staged.GetArtifactId(), DeadlineUnixMillis: deadline, ApprovedGrant: grant})
		if err != nil {
			return err
		}
		return waitBuiltinMCP(ctx, client, updated.GetOperation())
	}
	_, err = client.Install(ctx, &appliancev1.InstallRequest{InstallationId: builtinMCPInstallation, RequestId: requestID, ArtifactId: staged.GetArtifactId(), DeadlineUnixMillis: deadline, ApprovedGrant: grant})
	if err != nil {
		return err
	}
	installed, err := client.GetApplianceStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: builtinMCPInstallation})
	if err != nil {
		return err
	}
	activation, err := client.Activate(ctx, &appliancev1.ActivateRequest{InstallationId: builtinMCPInstallation, RequestId: newOperationID("mcp-activate"), ExpectedRevision: installed.GetStatus().GetRevision(), DeadlineUnixMillis: deadline})
	if err != nil {
		return err
	}
	return waitBuiltinMCP(ctx, client, activation.GetOperation())
}

func waitBuiltinMCP(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, operation *appliancev1.ApplianceOperation) error {
	if operation.GetState() == appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING {
		watch, err := client.WatchApplianceOperation(ctx, &appliancev1.WatchApplianceOperationRequest{InstallationId: builtinMCPInstallation, OperationId: operation.GetOperationId()})
		if err != nil {
			return err
		}
		for operation.GetState() == appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING {
			observed, err := watch.Recv()
			if err != nil {
				return err
			}
			operation = observed.GetOperation()
		}
	}
	if operation.GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED {
		return fmt.Errorf("MCP activation did not succeed: %s", operation.GetError().GetMessage())
	}
	return nil
}
