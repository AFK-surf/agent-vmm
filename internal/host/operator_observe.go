//go:build darwin || linux

package host

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *operatorServer) GetSystemSnapshot(ctx context.Context, _ *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return server.controller.systemSnapshot(ctx)
}

func (server *operatorServer) GetSystemHealthSummary(ctx context.Context, _ *hostv1.GetSystemHealthSummaryRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return server.controller.systemHealthSummary(ctx), nil
}

func (controller *Controller) systemSnapshot(ctx context.Context) (*hostv1.GetSystemSnapshotResponse, error) {
	response := controller.systemHealthSummary(ctx)
	if registrations, registrationErr := controller.store.ListRemoteRegistrations(ctx); registrationErr == nil {
		for _, registration := range registrations {
			response.RemoteRegistrations = append(response.RemoteRegistrations, controller.remoteRegistrationResponse(ctx, registration).GetRegistration())
		}
	}
	environments, err := controller.store.ListEnvironments(ctx)
	if err != nil {
		return nil, err
	}
	for _, environment := range environments {
		snapshot := controller.environmentSnapshot(ctx, environment)
		response.Environments = append(response.Environments, snapshot)
		addUsage(response, snapshot.GetUsage())
	}
	return response, nil
}

// systemHealthSummary performs a constant number of Host and Guest owner reads.
// It deliberately excludes per-environment and per-registration inventory.
func (controller *Controller) systemHealthSummary(ctx context.Context) *hostv1.GetSystemSnapshotResponse {
	response := &hostv1.GetSystemSnapshotResponse{
		Vm: controller.manager.Snapshot(), CollectedAt: timestamppb.Now(),
		HostResources:  controller.manager.ResourceSnapshot(),
		GuestResources: &hostv1.LayerResources{UnavailableReason: "guest control is unavailable"},
	}
	response.Health = &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_UNAVAILABLE}
	if response.Vm.GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return response
	}
	admission := controller.vmLifecycle.admissionHealth(response.CollectedAt)
	response.Health.Components = append(response.Health.Components, admission)
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return response
	}
	defer connection.Close()
	if usage, usageErr := client.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{}); usageErr == nil {
		response.GuestResources = guestLayerResources(usage)
	}
	health, err := client.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return response
	}
	response.Health.State = max(hostv1.HealthState_HEALTH_STATE_HEALTHY, admission.GetState())
	for _, component := range health.GetComponents() {
		state := hostv1.HealthState(component.GetState())
		response.Health.Components = append(response.Health.Components, &hostv1.ComponentHealth{Component: component.GetComponent(), State: state, Reason: component.GetReason(), ObservedAt: timestamppb.New(time.Unix(0, component.GetObservedAtUnixNano()))})
		if state > response.Health.State {
			response.Health.State = state
		}
	}
	hostEgress := controller.hostEgressHealth(response.Vm, response.CollectedAt)
	response.Health.Components = append(response.Health.Components, hostEgress)
	if hostEgress.GetState() > response.Health.State {
		response.Health.State = hostEgress.GetState()
	}
	return response
}

func (server *operatorServer) GetVMImpactSummary(ctx context.Context, _ *hostv1.GetVMImpactSummaryRequest) (*hostv1.GetVMImpactSummaryResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	return server.controller.vmImpactSummary(ctx)
}

func (controller *Controller) vmImpactSummary(ctx context.Context) (*hostv1.GetVMImpactSummaryResponse, error) {
	controller.inventoryTransition.Lock()
	defer controller.inventoryTransition.Unlock()
	return controller.vmImpactSummaryLocked(ctx), nil
}

func (controller *Controller) vmImpactSummaryLocked(ctx context.Context) *hostv1.GetVMImpactSummaryResponse {
	const limit = 100
	response := &hostv1.GetVMImpactSummaryResponse{Vm: controller.manager.Snapshot(), CollectedAt: timestamppb.Now(), Complete: true}
	appendReference := func(reference *hostv1.ResourceReference) {
		if len(response.KnownWorkloads) == limit {
			markImpactSummaryPartial(response, "known workload summary exceeds the 100 item bound", true)
			return
		}
		response.KnownWorkloads = append(response.KnownWorkloads, reference)
	}
	environments, err := controller.store.ListEnvironmentsBounded(ctx, limit+1)
	if err != nil {
		markImpactSummaryPartial(response, "environment summary is unavailable", false)
	} else {
		if len(environments) > limit {
			environments = environments[:limit]
			markImpactSummaryPartial(response, "environment summary exceeds the 100 item bound", true)
		}
		for _, environment := range environments {
			generation := environment.PolicyRevision
			if environment.OwnerKind == store.OwnerRemoteRegistration {
				generation = environment.AllocationGeneration
			}
			appendReference(&hostv1.ResourceReference{Kind: "environment", EnvironmentId: environment.ID, Generation: generation})
		}
	}
	registrations, err := controller.store.ListRemoteRegistrationsBounded(ctx, limit+1)
	if err != nil {
		markImpactSummaryPartial(response, "registration summary is unavailable", false)
	} else {
		if len(registrations) > limit {
			registrations = registrations[:limit]
			markImpactSummaryPartial(response, "registration summary exceeds the 100 item bound", true)
		}
		for _, registration := range registrations {
			appendReference(&hostv1.ResourceReference{Kind: "registration", ResourceId: registration.ID, Generation: registration.InventoryRevision})
		}
	}
	return response
}

func markImpactSummaryPartial(response *hostv1.GetVMImpactSummaryResponse, issue string, overflow bool) {
	response.Complete = false
	response.Overflow = response.Overflow || overflow
	if response.Issue == "" {
		response.Issue = issue
	}
}

func guestLayerResources(response *vmmdv1.GetUsageResponse) *hostv1.LayerResources {
	result := &hostv1.LayerResources{}
	var unavailable []string
	if usage := response.GetUsage(); usage != nil {
		if usage.GetUnavailable() {
			unavailable = append(unavailable, usage.GetUnavailableReason())
		} else {
			memory, pids := usage.GetMemoryBytes(), usage.GetPids()
			result.MemoryBytes, result.Pids = &memory, &pids
			result.SampledAt = timestamppb.New(time.Unix(0, usage.GetSampledAtUnixNano()))
		}
	}
	result.OpenFds = response.OpenFds
	for _, reason := range []string{response.GetOpenFdsUnavailableReason(), response.GetDiskUnavailableReason()} {
		if reason != "" {
			unavailable = append(unavailable, reason)
		}
	}
	if disk := response.GetDisk(); disk != nil {
		used, capacity := disk.GetUsedBytes(), disk.GetCapacityBytes()
		imports, reserve, metadata := disk.GetImportReservationBytes(), disk.GetSystemReserveBytes(), disk.GetMetadataReserveBytes()
		result.Disk = &hostv1.DiskAccounting{
			UsedBytes: &used, CapacityBytes: &capacity, ImportReservationBytes: &imports,
			SystemReserveBytes: &reserve, MetadataReserveBytes: &metadata,
		}
	}
	result.UnavailableReason = strings.Join(unavailable, "; ")
	return result
}

func (server *operatorServer) ListEnvironmentImages(ctx context.Context, request *hostv1.ListEnvironmentImagesRequest) (*hostv1.ListImagesResponse, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	environment, err := server.controller.store.GetEnvironment(ctx, request.GetEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "ENVIRONMENT_NOT_FOUND", "environment", request.GetEnvironmentId(), "agent environment does not exist")
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	response, err := client.ListImages(ctx, &vmmdv1.ListImagesRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	containers, err := client.ListContainers(ctx, &vmmdv1.ListContainersRequest{ActorNamespaceId: environment.NamespaceID})
	if err != nil {
		return nil, err
	}
	usedBy := make(map[string][]string)
	for _, container := range containers.GetContainers() {
		usedBy[container.GetImage()] = append(usedBy[container.GetImage()], container.GetId())
	}
	result := &hostv1.ListImagesResponse{}
	for _, image := range response.GetImages() {
		converted := imageFromGuest(image)
		converted.UsedByContainerIds = append([]string(nil), usedBy[image.GetReference()]...)
		result.Images = append(result.Images, converted)
	}
	return result, nil
}

func (controller *Controller) hostEgressHealth(vm *hostv1.VM, observedAt *timestamppb.Timestamp) *hostv1.ComponentHealth {
	active, bootID, revision, sources := controller.egress.Status()
	metrics := controller.egress.Metrics()
	state := hostv1.HealthState_HEALTH_STATE_HEALTHY
	reason := fmt.Sprintf("policy_revision=%d sources=%d packets_rx=%d packets_tx=%d drops=%d rejected=%d dial_failures=%d active_tcp=%d active_udp=%d active_dns=%d last_error=%q last_dial_error=%q", revision, sources, metrics.PacketsReceived, metrics.PacketsSent, metrics.PacketDrops, metrics.RejectedFlows, metrics.DialFailures, metrics.ActiveTCP, metrics.ActiveUDP, metrics.ActiveDNS, metrics.LastError, metrics.LastDialError)
	if !active || bootID != vm.GetBootId() || revision == 0 {
		state = hostv1.HealthState_HEALTH_STATE_DEGRADED
		reason = "egress session or source policy is not ready"
	}
	return &hostv1.ComponentHealth{Component: "host-egress", State: state, Reason: reason, ObservedAt: observedAt}
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
