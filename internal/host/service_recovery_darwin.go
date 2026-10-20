//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/egress"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (controller *Controller) revalidateServiceExportLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, exported *servicev1.ServiceExport, namespace string) (bool, error) {
	stored, err := controller.store.GetServiceExport(ctx, exported.GetExportId())
	if err != nil {
		return false, err
	}
	if stored.GetRevoked() {
		return false, nil
	}
	observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: namespace, ContainerId: stored.GetContainerId()})
	if err != nil && status.Code(err) != codes.NotFound {
		return false, err
	}
	container := observed.GetContainer()
	instance := ""
	stopped, err := controller.store.ServiceContainerStoppedByOwner(ctx, stored.GetEnvironmentId(), stored.GetContainerId(), container.GetGenerationId())
	if err != nil {
		return false, err
	}
	if !stopped && container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		instance = container.GetInstanceId()
	}
	imports, err := controller.store.ProjectServiceContainer(ctx, stored.GetEnvironmentId(), stored.GetContainerId(), container.GetGenerationId(), instance)
	if err != nil {
		return false, err
	}
	for _, id := range imports {
		controller.egress.DeleteServiceRoute(id)
		if _, err := client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: derivedRequestID(fmt.Sprintf("service-project-retire/%s/%s/%s", id, container.GetGenerationId(), instance)), ProjectionId: id}); err != nil && status.Code(err) != codes.NotFound {
			return false, err
		}
	}
	if instance == "" {
		return false, nil
	}
	exported.ContainerGenerationId = container.GetGenerationId()
	exported.ContainerInstanceId = instance
	return true, nil
}

func (controller *Controller) restoreLocalServiceProjectionsLocked(ctx context.Context, client vmmdv1.VMMDServiceClient) error {
	serviceProjections, err := controller.store.ListActiveServiceProjections(ctx, time.Now())
	if err != nil {
		return err
	}
	for _, projection := range serviceProjections {
		currentImport, err := controller.store.GetServiceImport(ctx, projection.Import.GetImportId())
		if err != nil {
			return err
		}
		if currentImport.GetRevoked() {
			continue
		}
		if projection.Import.GetSourceInstallationId() != "" {
			// The appliance owner publishes this route after its current instance is committed.
			continue
		}
		observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: projection.SourceNamespace, ContainerId: projection.Import.GetSourceContainerId()})
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		container := observed.GetContainer()
		instance := ""
		stopped, err := controller.store.ServiceContainerStoppedByOwner(ctx, projection.Import.GetSourceEnvironmentId(), projection.Import.GetSourceContainerId(), container.GetGenerationId())
		if err != nil {
			return err
		}
		if !stopped && container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			instance = container.GetInstanceId()
		}
		ids, err := controller.store.ProjectServiceContainer(ctx, projection.Import.GetSourceEnvironmentId(), projection.Import.GetSourceContainerId(), container.GetGenerationId(), instance)
		if err != nil {
			return err
		}
		for _, id := range ids {
			controller.egress.DeleteServiceRoute(id)
			if _, err := client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: derivedRequestID(fmt.Sprintf("service-source-retire/%s/%s/%s", id, container.GetGenerationId(), instance)), ProjectionId: id}); err != nil && status.Code(err) != codes.NotFound {
				return err
			}
		}
		if instance == "" {
			continue
		}
		projection.Import.SourceContainerGenerationId = container.GetGenerationId()
		projection.Import.SourceContainerInstanceId = instance
		current, err := controller.revalidateServiceExportLocked(ctx, client, projection.Export, projection.DestinationNamespace)
		if err != nil {
			return err
		}
		if !current {
			continue
		}
		record, err := controller.store.ServiceRouteForImport(ctx, projection.Import.GetImportId())
		if err != nil {
			return err
		}
		if strings.HasPrefix(record.Capability.GetCapabilityId(), "local-") && len(record.Capability.GetSignature()) == 0 {
			if record.Lease.GetExpiresAt().AsTime().Before(time.Now().Add(localServiceRouteLease / 2)) {
				capability := proto.Clone(record.Capability).(*trustv1.RouteCapability)
				capability.ExpiresAt = timestamppb.New(time.Now().Add(localServiceRouteLease))
				if err := controller.store.RenewLocalServiceProjectionLease(ctx, projection.Import.GetImportId(), capability); err != nil {
					return err
				}
				record.Lease.ExpiresAt = capability.ExpiresAt
			}
		} else if !record.Lease.GetExpiresAt().AsTime().After(time.Now()) {
			continue
		}
		projection.Import, projection.Export, projection.Generation, projection.ExpiresAt = record.Import, record.Export, record.Route.GetGeneration(), record.Lease.GetExpiresAt().AsTime()
		_, err = client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{
			RequestId: derivedRequestID(fmt.Sprintf("service-project/%s/%d/%s/%s/%d", projection.Import.GetImportId(), projection.Generation, projection.Import.GetSourceContainerInstanceId(), projection.Export.GetContainerInstanceId(), projection.ExpiresAt.UnixMilli())),
			Projection: &vmmdv1.ServiceProjection{Id: projection.Import.GetImportId(), SourceNamespaceId: projection.SourceNamespace,
				SourceContainerId: projection.Import.GetSourceContainerId(), SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(),
				DestinationNamespaceId: projection.DestinationNamespace, DestinationContainerId: projection.Export.GetContainerId(),
				DestinationContainerInstanceId: projection.Export.GetContainerInstanceId(), DestinationPort: projection.Export.GetPort(),
				DnsAlias: projection.Import.GetVirtualServiceName(), ExpiresUnixMillis: projection.ExpiresAt.UnixMilli()},
		})
		if err != nil {
			return err
		}
		if err := controller.store.ActivateServiceProjection(ctx, projection.Import.GetImportId(), projection.Generation); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) projectServiceContainerLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, environment, container, generation, instance string) (result error) {
	defer func() {
		result = errors.Join(result, controller.restoreContainerServiceListeners(ctx, environment, container))
	}()
	ids, err := controller.store.ProjectServiceContainer(ctx, environment, container, generation, instance)
	if err != nil {
		return err
	}
	for _, id := range ids {
		controller.egress.DeleteServiceRoute(id)
		if _, err := client.DeleteServiceProjection(ctx, &vmmdv1.DeleteServiceProjectionRequest{RequestId: derivedRequestID(fmt.Sprintf("service-project-retire/%s/%s/%s", id, generation, instance)), ProjectionId: id}); err != nil && status.Code(err) != codes.NotFound {
			return err
		}
	}
	return nil
}

func (controller *Controller) restoreServiceProjectionsLocked(ctx context.Context, client vmmdv1.VMMDServiceClient) (result error) {
	defer func() { result = errors.Join(result, controller.restoreLocalServiceListeners(ctx)) }()
	if err := controller.restoreLocalServiceProjectionsLocked(ctx, client); err != nil {
		return err
	}

	if controller.trust != nil {
		identity, identityErr := controller.trust.Identity(ctx)
		if identityErr != nil {
			return identityErr
		}
		remoteProjections, remoteErr := controller.store.ListActiveRemoteServiceProjections(ctx, identity.Identity.GetDeviceId(), time.Now())
		if remoteErr != nil {
			return remoteErr
		}
		for _, projection := range remoteProjections {
			observed, err := client.InspectContainer(ctx, &vmmdv1.InspectContainerRequest{ActorNamespaceId: projection.SourceNamespace, ContainerId: projection.Import.GetSourceContainerId()})
			if err != nil && status.Code(err) != codes.NotFound {
				return err
			}
			container := observed.GetContainer()
			instance := ""
			stopped, err := controller.store.ServiceContainerStoppedByOwner(ctx, projection.Import.GetSourceEnvironmentId(), projection.Import.GetSourceContainerId(), container.GetGenerationId())
			if err != nil {
				return err
			}
			if !stopped && container.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
				instance = container.GetInstanceId()
			}
			if err := controller.projectServiceContainerLocked(ctx, client, projection.Import.GetSourceEnvironmentId(), projection.Import.GetSourceContainerId(), container.GetGenerationId(), instance); err != nil {
				return err
			}
			if instance == "" {
				continue
			}
			record, err := controller.store.ServiceRouteForImport(ctx, projection.Import.GetImportId())
			if err != nil {
				return err
			}
			projection.Import, projection.Route, projection.Lease, projection.Capability = record.Import, record.Route, record.Lease, record.Capability
			endpoint, endpointErr := controller.trust.GetEndpointObservation(ctx, projection.Import.GetDestinationDeviceId())
			if endpointErr != nil {
				return endpointErr
			}
			virtualAddress := remoteServiceAddress(projection.Import.GetImportId())
			response, applyErr := client.ApplyServiceProjection(ctx, &vmmdv1.ApplyServiceProjectionRequest{
				RequestId: derivedRequestID(fmt.Sprintf("service-project/%s/%d/%s", projection.Import.GetImportId(), projection.Route.GetGeneration(), projection.Import.GetSourceContainerInstanceId())),
				Projection: &vmmdv1.ServiceProjection{Id: projection.Import.GetImportId(), SourceNamespaceId: projection.SourceNamespace,
					SourceContainerId: projection.Import.GetSourceContainerId(), SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(),
					DestinationPort: projection.Import.GetDestinationPort(), DnsAlias: projection.Import.GetVirtualServiceName(),
					ExpiresUnixMillis: projection.Lease.GetExpiresAt().AsTime().UnixMilli(), Remote: true, VirtualAddress: virtualAddress.String()},
			})
			if applyErr != nil {
				return applyErr
			}
			if err := controller.store.ActivateServiceProjection(ctx, projection.Import.GetImportId(), projection.Route.GetGeneration()); err != nil {
				return err
			}
			sourceAddress, parseErr := netip.ParseAddr(response.GetSourceAddress())
			if parseErr != nil {
				return parseErr
			}
			header := &servicev1.OpenServiceForwardHeader{RouteId: projection.Route.GetRouteId(), LeaseId: projection.Lease.GetLeaseId(),
				RouteGeneration: projection.Route.GetGeneration(), SourceEnvironmentId: projection.Import.GetSourceEnvironmentId(),
				SourceContainerInstanceId: projection.Import.GetSourceContainerInstanceId(), Capability: projection.Capability}
			if err := controller.egress.ApplyServiceRoute(egress.ServiceRoute{ID: projection.Import.GetImportId(), Generation: projection.Route.GetGeneration(), EnvironmentID: projection.Import.GetSourceEnvironmentId(),
				SourceAddress: sourceAddress, Destination: netip.AddrPortFrom(virtualAddress, uint16(projection.Import.GetDestinationPort())), ExpiresAt: projection.Lease.GetExpiresAt().AsTime(),
				Dial: func(routeContext context.Context) (net.Conn, error) {
					return controller.dialRemoteService(routeContext, header, endpoint)
				}}); err != nil {
				return err
			}
		}
	}
	return nil
}
