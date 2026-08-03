//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (server *operatorServer) ApplyNetworkLink(ctx context.Context, request *hostv1.ApplyNetworkLinkRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetLink().GetId() == "" {
		return nil, invalidField("request_id/link", "request ID and network link are required")
	}
	source, err := server.controller.store.GetEnvironment(ctx, request.GetLink().GetSourceEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "SOURCE_ENVIRONMENT_NOT_FOUND", "environment", request.GetLink().GetSourceEnvironmentId(), "source environment does not exist")
	}
	destination, err := server.controller.store.GetEnvironment(ctx, request.GetLink().GetDestinationEnvironmentId())
	if err != nil {
		return nil, resourceError(codes.NotFound, "DESTINATION_ENVIRONMENT_NOT_FOUND", "environment", request.GetLink().GetDestinationEnvironmentId(), "destination environment does not exist")
	}
	link, err := guestLinkFromHost(request.GetLink(), source.NamespaceID, destination.NamespaceID)
	if err != nil {
		return nil, err
	}
	payload, err := protojson.Marshal(request.GetLink())
	if err != nil {
		return nil, err
	}
	if err := server.controller.store.PutGrant(ctx, store.Grant{ID: request.GetLink().GetId(), Kind: "network-link", SourceNamespaceID: source.NamespaceID, TargetNamespaceID: destination.NamespaceID, Payload: string(payload)}); err != nil {
		return nil, err
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := client.ApplyNetworkLink(ctx, &vmmdv1.ApplyNetworkLinkRequest{RequestId: request.GetRequestId(), Link: link}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (server *operatorServer) DeleteNetworkLink(ctx context.Context, request *hostv1.DeleteNetworkLinkRequest) (*emptypb.Empty, error) {
	if _, err := auth.OperatorIdentity(ctx); err != nil {
		return nil, err
	}
	if request.GetRequestId() == "" || request.GetLinkId() == "" {
		return nil, invalidField("request_id/link_id", "request and link IDs are required")
	}
	grant, err := server.controller.store.GetGrant(ctx, request.GetLinkId())
	if errors.Is(err, sql.ErrNoRows) || err == nil && grant.Kind != "network-link" {
		return nil, resourceError(codes.NotFound, "NETWORK_LINK_NOT_FOUND", "network-link", request.GetLinkId(), "network link does not exist")
	} else if err != nil {
		return nil, err
	}
	if !grant.Revoked {
		if err := server.controller.store.RevokeGrant(ctx, request.GetLinkId()); err != nil {
			return nil, err
		}
	}
	client, connection, err := server.controller.guestClient(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err := client.DeleteNetworkLink(ctx, &vmmdv1.DeleteNetworkLinkRequest{RequestId: request.GetRequestId(), LinkId: request.GetLinkId()}); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func guestLinkFromPayload(payload, sourceNamespaceID, destinationNamespaceID string) (*vmmdv1.NetworkLink, error) {
	link := new(hostv1.NetworkLink)
	if err := protojson.Unmarshal([]byte(payload), link); err != nil {
		return nil, err
	}
	return guestLinkFromHost(link, sourceNamespaceID, destinationNamespaceID)
}

func guestLinkFromHost(link *hostv1.NetworkLink, sourceNamespaceID, destinationNamespaceID string) (*vmmdv1.NetworkLink, error) {
	if link == nil || link.GetId() == "" || link.GetSourceEnvironmentId() == "" || link.GetDestinationEnvironmentId() == "" || sourceNamespaceID == destinationNamespaceID {
		return nil, invalidField("link", "link ID and distinct source/destination environments are required")
	}
	if err := validateHostSelector(link.GetSource()); err != nil {
		return nil, invalidField("link.source", err.Error())
	}
	if err := validateHostSelector(link.GetDestination()); err != nil {
		return nil, invalidField("link.destination", err.Error())
	}
	if len(link.GetTcpPorts()) == 0 || len(link.GetTcpPorts()) > 256 {
		return nil, invalidField("link.tcp_ports", "one to 256 TCP ports are required")
	}
	seen := make(map[uint32]bool, len(link.GetTcpPorts()))
	for _, port := range link.GetTcpPorts() {
		if port == 0 || port > 65535 || seen[port] {
			return nil, invalidField("link.tcp_ports", "ports must be unique and between 1 and 65535")
		}
		seen[port] = true
	}
	alias := link.GetDnsAlias()
	if alias == "" || len(alias) > 63 || strings.Trim(alias, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" || alias[0] == '-' || alias[len(alias)-1] == '-' {
		return nil, invalidField("link.dns_alias", "DNS alias must be a lowercase DNS label")
	}
	return &vmmdv1.NetworkLink{
		Id: link.GetId(), SourceNamespaceId: sourceNamespaceID, DestinationNamespaceId: destinationNamespaceID,
		Source: selectorToGuest(link.GetSource()), Destination: selectorToGuest(link.GetDestination()), TcpPorts: link.GetTcpPorts(), DnsAlias: link.GetDnsAlias(),
	}, nil
}

func validateHostSelector(selector *hostv1.ContainerSelector) error {
	if selector == nil {
		return errors.New("selector is required")
	}
	switch selection := selector.GetSelection().(type) {
	case *hostv1.ContainerSelector_AllContainers:
		if !selection.AllContainers {
			return errors.New("all_containers must be true")
		}
	case *hostv1.ContainerSelector_ContainerIds:
		if selection.ContainerIds == nil || len(selection.ContainerIds.GetIds()) == 0 {
			return errors.New("container_ids must be non-empty")
		}
		seen := make(map[string]bool, len(selection.ContainerIds.GetIds()))
		for _, id := range selection.ContainerIds.GetIds() {
			if id == "" || seen[id] {
				return errors.New("container IDs must be non-empty and unique")
			}
			seen[id] = true
		}
	default:
		return errors.New("selector choice is required")
	}
	return nil
}

func selectorToGuest(selector *hostv1.ContainerSelector) *vmmdv1.ContainerSelector {
	result := new(vmmdv1.ContainerSelector)
	switch selection := selector.GetSelection().(type) {
	case *hostv1.ContainerSelector_AllContainers:
		result.Selection = &vmmdv1.ContainerSelector_AllContainers{AllContainers: selection.AllContainers}
	case *hostv1.ContainerSelector_ContainerIds:
		result.Selection = &vmmdv1.ContainerSelector_ContainerIds{ContainerIds: &vmmdv1.ContainerIDList{Ids: selection.ContainerIds.GetIds()}}
	}
	return result
}
