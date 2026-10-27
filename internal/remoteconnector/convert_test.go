package remoteconnector

import (
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestRemoteContractConversion(t *testing.T) {
	for _, enums := range []struct {
		name         string
		host, remote map[int32]string
	}{
		{"egress", hostv1.EgressMode_name, remotev1.EgressMode_name},
		{"allocation state", hostv1.AllocationState_name, remotev1.AllocationState_name},
		{"cleanup kind", hostv1.CleanupKind_name, remotev1.CleanupKind_name},
	} {
		if len(enums.host) != len(enums.remote) {
			t.Fatalf("%s values differ", enums.name)
		}
		for number, name := range enums.host {
			if enums.remote[number] != name {
				t.Fatalf("%s value %d: %s != %s", enums.name, number, name, enums.remote[number])
			}
		}
	}
	for _, limits := range []*hostv1.ResourceLimits{nil, {}, {Pids: proto.Uint32(0)}, {Pids: proto.Uint32(128), DiskBytes: proto.Uint64(1 << 30)}} {
		if got := limitsToHost(limitsToRemote(limits)); !proto.Equal(got, limits) {
			t.Fatalf("limits round trip: %v != %v", got, limits)
		}
	}
}
