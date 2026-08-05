//go:build darwin

package host

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
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
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(time.Now().Add(30*time.Second)) {
		deadline = time.Now().Add(30 * time.Second)
	}
	diagnosticCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	lease, err := server.controller.egress.Leases.Acquire(deadline)
	if err != nil {
		server.controller.failOperation(ctx, &operation, "DIAGNOSTIC_LIMIT", err)
		return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
	}
	defer lease.Release()
	results := server.diagnose(diagnosticCtx, request.GetEnvironmentId())
	degraded := false
	for _, result := range results {
		if result.State == "unavailable" {
			server.controller.failOperation(ctx, &operation, "DIAGNOSTIC_FAILED", errors.New(result.Layer+": "+result.Reason))
			return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
		}
		degraded = degraded || result.State == "degraded"
	}
	if degraded {
		server.controller.completeOperation(ctx, &operation, "degraded")
	} else {
		server.controller.completeOperation(ctx, &operation, "complete")
	}
	return &hostv1.OperationResponse{Operation: operationFromStore(operation)}, nil
}

func (server *operatorServer) diagnose(ctx context.Context, environmentID string) []diagnosticResult {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	results := []diagnosticResult{{Layer: "sidecar_sqlite", State: "healthy", ObservedAt: now}}
	vm := server.controller.manager.Snapshot()
	if vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return append(results, diagnosticResult{Layer: "host_netstack", State: "unavailable", Reason: vm.GetState().String(), ObservedAt: now})
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return append(results, diagnosticResult{Layer: "vsock_session", State: "unavailable", Reason: "guest control connection failed", ObservedAt: now})
	}
	defer connection.Close()
	health, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return append(results, diagnosticResult{Layer: "vsock_session", State: "unavailable", Reason: "guest health probe failed", ObservedAt: now})
	}
	components := make(map[string]*vmmdv1.ComponentStatus)
	for _, component := range health.GetComponents() {
		components[component.GetComponent()] = component
	}
	results = append(results, componentDiagnostic("guest_route", components["network"], now))
	results = append(results, componentDiagnostic("vsock_session", components["egress"], now))
	active, bootID, revision, sourceCount := server.controller.egress.Status()
	if !active || bootID != vm.GetBootId() {
		results = append(results, diagnosticResult{Layer: "host_netstack", State: "unavailable", Reason: "boot-scoped egress session is not active", ObservedAt: now})
	} else {
		results = append(results, diagnosticResult{Layer: "host_netstack", State: "healthy", ObservedAt: now})
	}
	if revision == 0 {
		results = append(results, diagnosticResult{Layer: "source_policy", State: "unavailable", Reason: "source policy is not published", ObservedAt: now})
	} else {
		results = append(results, diagnosticResult{Layer: "source_policy", State: "healthy", Reason: fmt.Sprintf("revision %d, %d sources", revision, sourceCount), ObservedAt: now})
	}
	dnsErr, dialErr := probeHostNetwork(ctx)
	results = append(results, diagnosticError("host_dns", dnsErr, now), diagnosticError("host_dial", dialErr, now))
	results = append(results, diagnosticResult{Layer: "guest_policy_dns", State: "not_applicable", Reason: "no environment selected", ObservedAt: now})
	if environmentID != "" {
		environment, err := server.controller.store.GetEnvironment(ctx, environmentID)
		if err != nil {
			return append(results, diagnosticResult{Layer: "source_policy", State: "unavailable", Reason: "environment projection is missing", ObservedAt: now})
		}
		usage, err := client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
		if err != nil || usage.GetUsage().GetUnavailable() {
			reason := usage.GetUsage().GetUnavailableReason()
			if err != nil {
				reason = err.Error()
			}
			setDiagnostic(results, "guest_route", "unavailable", reason)
		}
		probe, probeErr := client.ProbeEgress(ctx, &vmmdv1.ProbeEgressRequest{NamespaceId: environment.NamespaceID})
		if probeErr != nil {
			setDiagnostic(results, "guest_policy_dns", "degraded", "namespace egress probe failed")
		} else if probe.GetFailureLayer() != "" {
			setDiagnostic(results, probe.GetFailureLayer(), "degraded", probe.GetReason())
			if probe.GetFailureLayer() != "guest_policy_dns" {
				setDiagnostic(results, "guest_policy_dns", "healthy", "")
			}
		} else {
			setDiagnostic(results, "guest_policy_dns", "healthy", "")
			if !probe.GetPolicyEnforced() || !probe.GetPrivateDenied() {
				setDiagnostic(results, "source_policy", "degraded", "negative destination probe did not prove fail-closed policy")
			}
		}
	}
	return results
}

func setDiagnostic(results []diagnosticResult, layer, state, reason string) {
	for index := range results {
		if results[index].Layer == layer {
			results[index].State = state
			results[index].Reason = reason
			return
		}
	}
}

func componentDiagnostic(layer string, component *vmmdv1.ComponentStatus, observedAt string) diagnosticResult {
	if component == nil {
		return diagnosticResult{Layer: layer, State: "unavailable", Reason: "component did not report status", ObservedAt: observedAt}
	}
	state := "healthy"
	if component.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_HEALTHY {
		state = "unavailable"
	}
	return diagnosticResult{Layer: layer, State: state, Reason: component.GetReason(), ObservedAt: observedAt}
}

func diagnosticError(layer string, err error, observedAt string) diagnosticResult {
	if err != nil {
		return diagnosticResult{Layer: layer, State: "degraded", Reason: err.Error(), ObservedAt: observedAt}
	}
	return diagnosticResult{Layer: layer, State: "healthy", ObservedAt: observedAt}
}

func probeHostNetwork(ctx context.Context) (dnsErr, dialErr error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupNetIP(probeCtx, "ip4", "example.com")
	if err != nil {
		return errors.New("system resolver probe failed"), errors.New("dial probe skipped after DNS failure")
	}
	var target netip.Addr
	for _, address := range addresses {
		address = address.Unmap()
		if egress.DestinationAllowed(address, nil, true) {
			target = address
			break
		}
	}
	if !target.IsValid() {
		return errors.New("system resolver returned no permitted IPv4 address"), errors.New("dial probe skipped after DNS filtering")
	}
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(probeCtx, "tcp4", net.JoinHostPort(target.String(), "443"))
	if err != nil {
		return nil, errors.New("public TCP dial probe failed")
	}
	_ = connection.Close()
	return nil, nil
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
