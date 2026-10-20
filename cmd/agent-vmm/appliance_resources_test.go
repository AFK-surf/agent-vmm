//go:build darwin

package main

import (
	"flag"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func TestApplianceResourceApproval(t *testing.T) {
	limits := &hostv1.ResourceLimits{Pids: proto.Uint32(1024), DiskBytes: proto.Uint64(16 << 30)}
	manifest := &appliancev1.ControllerApplianceManifest{SystemLimits: limits, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: limits}}
	for _, test := range []struct {
		name string
		args []string
		fail bool
	}{
		{"explicit", []string{"--environment-disk-gib=4", "--system-pids=256"}, false},
		{"zero", []string{"--environment-disk-gib=0"}, true},
		{"overflow", []string{"--environment-disk-gib=18446744073709551615"}, true},
		{"manifest", []string{"--system-pids=1025"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet(test.name, flag.ContinueOnError)
			resources := registerApplianceResources(flags)
			if err := flags.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			flags.Visit(func(f *flag.Flag) { resources.record(f.Name) })
			previous, err := lifecycleGrant(manifest, nil, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			previous.Capacity.PerEnvironmentLimits.DiskBytes = proto.Uint64(8 << 30)
			grant, err := lifecycleGrant(manifest, previous, nil, false)
			if err != nil {
				t.Fatal(err)
			}
			err = resources.apply(grant, manifest)
			if (err != nil) != test.fail {
				t.Fatalf("error=%v", err)
			}
			if !test.fail && (grant.GetCapacity().GetPerEnvironmentLimits().GetDiskBytes() != 4<<30 || grant.GetSystemLimits().GetPids() != 256 || grant.GetSystemLimits().GetDiskBytes() != 16<<30 || previous.GetCapacity().GetPerEnvironmentLimits().GetDiskBytes() != 8<<30) {
				t.Fatal("explicit approval changed unprovided or original fields")
			}
		})
	}
}
