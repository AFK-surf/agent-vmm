//go:build darwin

package host

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
)

type diagnosticResult struct {
	Layer      string `json:"layer"`
	State      string `json:"state"`
	Reason     string `json:"reason,omitempty"`
	ObservedAt string `json:"observed_at"`
}

func (server *operatorServer) GetOperation(ctx context.Context, request *hostv1.GetOperationRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	operation, err := server.controller.store.GetOperation(ctx, request.GetOperationId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "OPERATION_NOT_FOUND", "operation", request.GetOperationId(), "operation does not exist")
	}
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) RunDiagnostics(ctx context.Context, request *hostv1.RunDiagnosticsRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	operation, existing, err := server.controller.beginOperation(ctx, "diagnostic", request.GetEnvironmentId(), request.GetRequestId())
	if err != nil {
		return nil, err
	}
	if existing {
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	results := server.diagnose(ctx, request.GetEnvironmentId())
	for _, result := range results {
		if result.State == "unavailable" {
			server.controller.failOperation(ctx, &operation, "DIAGNOSTIC_FAILED", errors.New(result.Layer+": "+result.Reason))
			return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
		}
	}
	server.controller.completeOperation(ctx, &operation, "complete")
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) diagnose(ctx context.Context, environmentID string) []diagnosticResult {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	results := []diagnosticResult{{Layer: "sidecar/sqlite", State: "healthy", ObservedAt: now}}
	vm := server.controller.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return append(results, diagnosticResult{Layer: "VZ", State: "unavailable", Reason: vm.GetState().String(), ObservedAt: now})
	}
	results = append(results, diagnosticResult{Layer: "bundle/disk/VZ", State: "healthy", ObservedAt: now})
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return append(results, diagnosticResult{Layer: "vsock/vmmd", State: "unavailable", Reason: err.Error(), ObservedAt: now})
	}
	defer connection.Close()
	health, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return append(results, diagnosticResult{Layer: "vmmd", State: "unavailable", Reason: err.Error(), ObservedAt: now})
	}
	for _, component := range health.GetComponents() {
		state := "healthy"
		if component.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_HEALTHY {
			state = "unavailable"
		}
		results = append(results, diagnosticResult{Layer: component.GetComponent(), State: state, Reason: component.GetReason(), ObservedAt: now})
	}
	if environmentID != "" {
		environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
		if err != nil {
			return append(results, diagnosticResult{Layer: "environment", State: "unavailable", Reason: err.Error(), ObservedAt: now})
		}
		usage, err := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
		if err != nil || usage.GetUsage().GetUnavailable() {
			reason := usage.GetUsage().GetUnavailableReason()
			if err != nil {
				reason = err.Error()
			}
			results = append(results, diagnosticResult{Layer: "namespace quota/network", State: "unavailable", Reason: reason, ObservedAt: now})
		} else {
			results = append(results, diagnosticResult{Layer: "namespace quota/network", State: "healthy", ObservedAt: now})
		}
	}
	return results
}

func (server *operatorServer) PreviewSupportBundle(ctx context.Context, _ *hostv1.PreviewSupportBundleRequest) (*hostv1.SupportBundlePreview, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return &hostv1.SupportBundlePreview{Items: []*hostv1.SupportBundleItem{
		{Name: "diagnostics.json", Description: "Layered health results", IncludedByDefault: true},
		{Name: "environment-policy.json", Description: "Limits and redacted access policy", IncludedByDefault: true},
		{Name: "runtime-summary.json", Description: "Resource identifiers, state, and usage without environment values", IncludedByDefault: true},
		{Name: "recent-logs", Description: "User-selected log records", Sensitive: true},
	}}, nil
}

func (server *operatorServer) ExportSupportBundle(ctx context.Context, request *hostv1.ExportSupportBundleRequest) (*hostv1.OperationResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	operation, existing, err := server.controller.beginOperation(ctx, "support-bundle", request.GetEnvironmentId(), request.GetRequestId())
	if err != nil {
		return nil, err
	}
	if existing {
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	path := filepath.Join(server.controller.supportDirectory, operation.ID+".zip")
	if err := server.writeSupportBundle(ctx, path, request.GetEnvironmentId(), request.GetIncludeSensitiveItems()); err != nil {
		server.controller.failOperation(ctx, &operation, "SUPPORT_BUNDLE_FAILED", err)
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	operation.OutputPath = path
	server.controller.completeOperation(ctx, &operation, "written")
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) writeSupportBundle(ctx context.Context, path, environmentID string, sensitiveItems []string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	archive := zip.NewWriter(file)
	closeWithError := func(current error) error {
		return errors.Join(current, archive.Close(), file.Close())
	}
	if err := writeJSON(archive, "diagnostics.json", server.diagnose(ctx, environmentID)); err != nil {
		return closeWithError(err)
	}
	if environmentID != "" {
		environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
		if err != nil {
			return closeWithError(err)
		}
		if err := writeJSON(archive, "environment-policy.json", environmentFromStore(environment)); err != nil {
			return closeWithError(err)
		}
		if err := writeJSON(archive, "runtime-summary.json", server.controller.environmentSnapshot(ctx, environment)); err != nil {
			return closeWithError(err)
		}
		if contains(sensitiveItems, "recent-logs") {
			if err := server.writeRecentLogs(ctx, archive, environment); err != nil {
				return closeWithError(err)
			}
		}
	}
	return closeWithError(nil)
}

func (server *operatorServer) writeRecentLogs(ctx context.Context, archive *zip.Writer, environment store.Environment) error {
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return err
	}
	for _, container := range containers.GetContainers() {
		stream, err := client.Logs(ctx, &vmmdv1.LogsRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: container.GetId(), TailRecords: 500})
		if err != nil {
			return err
		}
		writer, err := archive.Create(filepath.Join("recent-logs", container.GetId()+".jsonl"))
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(writer)
		for {
			record, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err := encoder.Encode(record); err != nil {
				return err
			}
		}
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func writeJSON(archive *zip.Writer, name string, value any) error {
	writer, err := archive.Create(name)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
