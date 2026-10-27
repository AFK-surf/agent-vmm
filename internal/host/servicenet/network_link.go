//go:build darwin || linux

package servicenet

import (
	"errors"
	"strings"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func GuestLinkFromPayload(payload, sourceNamespaceID, destinationNamespaceID string) (*vmmdv1.NetworkLink, error) {
	link := new(hostv1.NetworkLink)
	if err := protojson.Unmarshal([]byte(payload), link); err != nil {
		return nil, err
	}
	return GuestLinkFromHost(link, sourceNamespaceID, destinationNamespaceID)
}

func GuestLinkFromHost(link *hostv1.NetworkLink, sourceNamespaceID, destinationNamespaceID string) (*vmmdv1.NetworkLink, error) {
	if link == nil || link.GetId() == "" || link.GetSourceEnvironmentId() == "" || link.GetDestinationEnvironmentId() == "" || sourceNamespaceID == destinationNamespaceID {
		return nil, hostv1.InvalidField("link", "link ID and distinct source/destination environments are required")
	}
	if err := validateHostSelector(link.GetSource()); err != nil {
		return nil, hostv1.InvalidField("link.source", err.Error())
	}
	if err := validateHostSelector(link.GetDestination()); err != nil {
		return nil, hostv1.InvalidField("link.destination", err.Error())
	}
	if len(link.GetTcpPorts()) == 0 || len(link.GetTcpPorts()) > 256 {
		return nil, hostv1.InvalidField("link.tcp_ports", "one to 256 TCP ports are required")
	}
	seen := make(map[uint32]bool, len(link.GetTcpPorts()))
	for _, port := range link.GetTcpPorts() {
		if port == 0 || port > 65535 || seen[port] {
			return nil, hostv1.InvalidField("link.tcp_ports", "ports must be unique and between 1 and 65535")
		}
		seen[port] = true
	}
	alias := link.GetDnsAlias()
	if alias == "" || len(alias) > 63 || strings.Trim(alias, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" || alias[0] == '-' || alias[len(alias)-1] == '-' {
		return nil, hostv1.InvalidField("link.dns_alias", "DNS alias must be a lowercase DNS label")
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
