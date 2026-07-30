//go:build linux

package network

import (
	"errors"

	"github.com/ti-mo/conntrack"
)

func deleteLinkConntrack(sources, destinations []Endpoint, ports []uint32) error {
	sourceAddresses := make(map[string]bool, len(sources))
	destinationAddresses := make(map[string]bool, len(destinations))
	allowedPorts := make(map[uint16]bool, len(ports))
	for _, endpoint := range sources {
		sourceAddresses[endpoint.Address.String()] = true
	}
	for _, endpoint := range destinations {
		destinationAddresses[endpoint.Address.String()] = true
	}
	for _, port := range ports {
		allowedPorts[uint16(port)] = true
	}
	connection, err := conntrack.Dial(nil)
	if err != nil {
		return err
	}
	defer connection.Close()
	flows, err := connection.Dump(nil)
	if err != nil {
		return err
	}
	var result error
	for _, flow := range flows {
		tuple := flow.TupleOrig
		if tuple.Proto.Protocol == 6 && sourceAddresses[tuple.IP.SourceAddress.String()] && destinationAddresses[tuple.IP.DestinationAddress.String()] && allowedPorts[tuple.Proto.DestinationPort] {
			if err := connection.Delete(flow); err != nil {
				result = errors.Join(result, err)
			}
		}
	}
	return result
}
