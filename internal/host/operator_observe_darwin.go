//go:build darwin

package host

import (
	"context"
	"fmt"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) GetSystemSnapshot(ctx context.Context, _ *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	response := &hostv1.GetSystemSnapshotResponse{Vm: server.controller.manager.Snapshot(), CollectedAt: timestamppb.Now()}
	environments, err := server.controller.store.ListEnvironments(ctx)
	if err != nil {
		return nil, err
	}
	for _, environment := range environments {
		snapshot := server.controller.environmentSnapshot(ctx, environment)
		response.Environments = append(response.Environments, snapshot)
		addUsage(response, snapshot.GetUsage())
	}
	response.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
	if response.Vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return response, nil
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return response, nil
	}
	defer connection.Close()
	if systemUsage, usageErr := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{}); usageErr == nil {
		addUsage(response, usageFromGuest(systemUsage.GetUsage()))
	} else {
		markUsageStale(response)
	}
	health, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return response, nil
	}
	response.Health.State = hostv1.HealthState_HEALTH_STATE_HEALTHY
	for _, component := range health.GetComponents() {
		state := hostv1.HealthState(component.GetState())
		response.Health.Components = append(response.Health.Components, &hostv1.ComponentHealth{Component: component.GetComponent(), State: state, Reason: component.GetReason(), ObservedAt: timestamppb.New(time.Unix(0, component.GetObservedAtUnixNano()))})
		if state > response.Health.State {
			response.Health.State = state
		}
	}
	hostEgress := server.controller.hostEgressHealth(response.Vm, response.CollectedAt)
	response.Health.Components = append(response.Health.Components, hostEgress)
	if hostEgress.GetState() > response.Health.State {
		response.Health.State = hostEgress.GetState()
	}
	return response, nil
}

func (controller *Controller) hostEgressHealth(vm *hostv1.VM, observedAt *timestamppb.Timestamp) *hostv1.ComponentHealth {
	active, bootID, revision, sources := controller.egress.Status()
	metrics := controller.egress.Metrics()
	state := hostv1.HealthState_HEALTH_STATE_HEALTHY
	reason := fmt.Sprintf("policy_revision=%d sources=%d packets_rx=%d packets_tx=%d drops=%d rejected=%d active_tcp=%d active_udp=%d active_dns=%d", revision, sources, metrics.PacketsReceived, metrics.PacketsSent, metrics.PacketDrops, metrics.RejectedFlows, metrics.ActiveTCP, metrics.ActiveUDP, metrics.ActiveDNS)
	if !active || bootID != vm.GetBootId() || revision == 0 {
		state = hostv1.HealthState_HEALTH_STATE_DEGRADED
		reason = "egress session or source policy is not ready"
	}
	return &hostv1.ComponentHealth{Component: "host-egress", State: state, Reason: reason, ObservedAt: observedAt}
}

func markUsageStale(response *hostv1.GetSystemSnapshotResponse) {
	if response.Usage == nil {
		response.Usage = &hostv1.Usage{}
	}
	if response.Usage.GetSampledAt() == nil {
		response.Usage.SampledAt = timestamppb.Now()
	}
	response.Usage.Stale = true
}

func addUsage(response *hostv1.GetSystemSnapshotResponse, usage *hostv1.Usage) {
	if usage == nil {
		return
	}
	if response.Usage == nil {
		response.Usage = &hostv1.Usage{SampledAt: usage.GetSampledAt()}
	}
	response.Usage.CpuNanos += usage.GetCpuNanos()
	response.Usage.MemoryBytes += usage.GetMemoryBytes()
	response.Usage.Pids += usage.GetPids()
	response.Usage.ReadBytes += usage.GetReadBytes()
	response.Usage.WriteBytes += usage.GetWriteBytes()
	response.Usage.DiskBytes += usage.GetDiskBytes()
	response.Usage.NetworkRxBytes += usage.GetNetworkRxBytes()
	response.Usage.NetworkTxBytes += usage.GetNetworkTxBytes()
	response.Usage.Stale = response.Usage.Stale || usage.GetStale()
}

func (server *operatorServer) WatchEvents(request *hostv1.WatchEventsRequest, stream hostv1.OperatorService_WatchEventsServer) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.WatchEvents(stream.Context(), &vmmdv1.WatchEventsRequest{AfterSequence: request.GetAfterSequence()})
	if err != nil {
		return err
	}
	for {
		event, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resource := &hostv1.ResourceReference{Kind: event.GetResource().GetKind(), ResourceId: event.GetResource().GetResourceId()}
		if namespaceID := event.GetResource().GetNamespaceId(); namespaceID != "" {
			if environment, lookupErr := server.controller.store.EnvironmentByNamespace(stream.Context(), namespaceID); lookupErr == nil {
				resource.EnvironmentId = environment.ID
			}
		}
		if err := stream.Send(&hostv1.RuntimeEvent{Sequence: event.GetSequence(), Type: event.GetType(), Resource: resource, GuestBootId: event.GetGuestBootId(), RequestId: event.GetRequestId(), OperationId: event.GetOperationId(), Timestamp: timestamppb.New(time.Unix(0, event.GetTimestampUnixNano()))}); err != nil {
			return err
		}
	}
}

func (server *operatorServer) WatchMetrics(request *hostv1.WatchMetricsRequest, stream hostv1.OperatorService_WatchMetricsServer) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	interval := time.Second
	if request.GetInterval() != nil {
		interval = request.GetInterval().AsDuration()
	}
	if interval < 500*time.Millisecond || interval > time.Minute {
		return invalidField("interval", "metrics interval must be between 500ms and 1m")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		usage, err := server.collectMetrics(stream.Context(), request.GetScope())
		if err != nil {
			return err
		}
		if err := stream.Send(&hostv1.MetricsSample{Scope: request.GetScope(), Usage: usage}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

func (server *operatorServer) collectMetrics(ctx context.Context, scope *hostv1.ResourceReference) (*hostv1.Usage, error) {
	if scope == nil || scope.GetEnvironmentId() == "" {
		snapshot, err := server.GetSystemSnapshot(ctx, &hostv1.GetSystemSnapshotRequest{})
		if err != nil {
			return nil, err
		}
		if snapshot.GetUsage() == nil {
			return &hostv1.Usage{SampledAt: timestamppb.Now(), Stale: true}, nil
		}
		return snapshot.GetUsage(), nil
	}
	environment, err := server.controller.store.GetEnvironment(ctx, scope.GetEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", scope.GetEnvironmentId(), "agent environment does not exist")
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	var response *vmmdv1.GetUsageResponse
	if scope.GetKind() == "container" && scope.GetResourceId() != "" {
		response, err = client.GetContainerUsage(ctx, &vmmdv1.GetContainerUsageRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: scope.GetResourceId()})
	} else {
		response, err = client.GetNamespaceUsage(ctx, &vmmdv1.GetNamespaceUsageRequest{NamespaceId: environment.NamespaceID})
	}
	if err != nil {
		return nil, err
	}
	return usageFromGuest(response.GetUsage()), nil
}

func (server *operatorServer) Logs(request *hostv1.OperatorLogsRequest, stream hostv1.OperatorService_LogsServer) error {
	if _, err := auth.OperatorIdentity(stream.Context()); err != nil {
		return err
	}
	environment, err := server.controller.store.GetEnvironment(stream.Context(), request.GetEnvironmentId())
	if err != nil {
		return resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
	}
	query := request.GetQuery()
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guestRequest := &vmmdv1.LogsRequest{ActorNamespaceId: environment.NamespaceID, ContainerId: query.GetContainerId(), TailRecords: query.GetTailRecords(), AfterCursor: query.GetAfterCursor(), Follow: query.GetFollow()}
	if query.GetSince() != nil {
		guestRequest.SinceUnixNano = query.GetSince().AsTime().UnixNano()
	}
	guest, err := client.Logs(stream.Context(), guestRequest)
	if err != nil {
		return err
	}
	for {
		record, err := guest.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&hostv1.LogRecord{Cursor: record.GetCursor(), Timestamp: timestamppb.New(time.Unix(0, record.GetTimestampUnixNano())), Stream: hostv1.LogStream(record.GetStream()), Data: record.GetData(), Truncated: record.GetTruncated(), DroppedRecords: record.GetDroppedRecords()}); err != nil {
			return err
		}
	}
}
