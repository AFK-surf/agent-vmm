//go:build darwin || linux

package lifecycle

import (
	"context"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type builtinMCPClient struct {
	appliancev1.ApplianceAdminServiceClient
	current                       *appliancev1.ApplianceStatus
	installed, activated, updated bool
}

func (client *builtinMCPClient) StageArtifact(context.Context, *appliancev1.StageArtifactRequest, ...grpc.CallOption) (*appliancev1.StageArtifactResponse, error) {
	return &appliancev1.StageArtifactResponse{ArtifactId: "artifact"}, nil
}
func (client *builtinMCPClient) GetApplianceStatus(context.Context, *appliancev1.GetApplianceStatusRequest, ...grpc.CallOption) (*appliancev1.GetApplianceStatusResponse, error) {
	if client.current == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return &appliancev1.GetApplianceStatusResponse{Status: client.current}, nil
}
func (client *builtinMCPClient) Install(_ context.Context, request *appliancev1.InstallRequest, _ ...grpc.CallOption) (*appliancev1.InstallResponse, error) {
	client.installed = true
	client.current = &appliancev1.ApplianceStatus{InstallationId: request.GetInstallationId(), ArtifactId: request.GetArtifactId(), Revision: 1}
	return &appliancev1.InstallResponse{}, nil
}
func (client *builtinMCPClient) Activate(_ context.Context, request *appliancev1.ActivateRequest, _ ...grpc.CallOption) (*appliancev1.ActivateResponse, error) {
	client.activated = request.GetExpectedRevision() == client.current.GetRevision()
	return &appliancev1.ActivateResponse{Operation: &appliancev1.ApplianceOperation{State: appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED}}, nil
}
func (client *builtinMCPClient) UpdateAppliance(_ context.Context, request *appliancev1.UpdateApplianceRequest, _ ...grpc.CallOption) (*appliancev1.UpdateApplianceResponse, error) {
	client.updated = request.GetExpectedRevision() == client.current.GetRevision()
	return &appliancev1.UpdateApplianceResponse{Operation: &appliancev1.ApplianceOperation{State: appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED}}, nil
}
func TestBuiltinMCPInstallsActivatesAndPreservesCurrentArtifact(t *testing.T) {
	for _, test := range []struct {
		name, artifact            string
		install, activate, update bool
	}{
		{name: "new", install: true, activate: true},
		{name: "current", artifact: "artifact"},
		{name: "changed", artifact: "old", update: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &builtinMCPClient{}
			if test.artifact != "" {
				client.current = &appliancev1.ApplianceStatus{ArtifactId: test.artifact, Revision: 7}
			}
			if err := applyBuiltinMCP(t.Context(), client, &appliancev1.ControllerApplianceManifest{}); err != nil {
				t.Fatal(err)
			}
			if client.installed != test.install || client.activated != test.activate || client.updated != test.update {
				t.Fatalf("install=%t activate=%t update=%t", client.installed, client.activated, client.updated)
			}
		})
	}
}
