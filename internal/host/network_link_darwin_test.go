//go:build darwin

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func TestGuestLinkFromHostValidatesCompletePolicy(t *testing.T) {
	valid := &hostv1.NetworkLink{
		Id: "link-a", SourceEnvironmentId: "source", DestinationEnvironmentId: "destination",
		Source: allContainersSelector(), Destination: allContainersSelector(), TcpPorts: []uint32{443}, DnsAlias: "service",
	}
	if _, err := guestLinkFromHost(valid, "namespace-source", "namespace-destination"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit func(*hostv1.NetworkLink)
	}{
		{name: "same namespace", edit: func(link *hostv1.NetworkLink) { link.DestinationEnvironmentId = link.SourceEnvironmentId }},
		{name: "missing selector", edit: func(link *hostv1.NetworkLink) { link.Source = nil }},
		{name: "zero port", edit: func(link *hostv1.NetworkLink) { link.TcpPorts = []uint32{0} }},
		{name: "duplicate port", edit: func(link *hostv1.NetworkLink) { link.TcpPorts = []uint32{443, 443} }},
		{name: "invalid alias", edit: func(link *hostv1.NetworkLink) { link.DnsAlias = "Invalid_Alias" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			link := proto.Clone(valid).(*hostv1.NetworkLink)
			test.edit(link)
			destinationNamespace := "namespace-destination"
			if link.DestinationEnvironmentId == link.SourceEnvironmentId {
				destinationNamespace = "namespace-source"
			}
			if _, err := guestLinkFromHost(link, "namespace-source", destinationNamespace); err == nil {
				t.Fatal("invalid link accepted")
			}
		})
	}
}

func allContainersSelector() *hostv1.ContainerSelector {
	return &hostv1.ContainerSelector{Selection: &hostv1.ContainerSelector_AllContainers{AllContainers: true}}
}
