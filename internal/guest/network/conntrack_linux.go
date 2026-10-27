//go:build linux

package network

import (
	"errors"

	"github.com/ti-mo/conntrack"
)

func deleteRevokedConntrack(tuples map[revokedTuple]struct{}) error {
	if len(tuples) == 0 {
		return nil
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
		_, revoked := tuples[revokedTuple{tuple.IP.SourceAddress, tuple.IP.DestinationAddress, tuple.Proto.DestinationPort}]
		if tuple.Proto.Protocol == 6 && revoked {
			if err := connection.Delete(flow); err != nil {
				result = errors.Join(result, err)
			}
		}
	}
	return result
}
