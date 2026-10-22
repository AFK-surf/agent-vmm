//go:build darwin || linux

package main

import (
	"flag"
	"fmt"
	"math"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type applianceResources struct {
	values   map[string]*uint64
	provided map[string]bool
}

func registerApplianceResources(flags *flag.FlagSet) *applianceResources {
	r := &applianceResources{values: make(map[string]*uint64), provided: make(map[string]bool)}
	for _, scope := range []string{"system", "environment"} {
		r.values[scope+"-pids"] = flags.Uint64(scope+"-pids", 0, "approved positive PID limit, bounded by manifest")
		r.values[scope+"-disk-gib"] = flags.Uint64(scope+"-disk-gib", 0, "approved positive writable disk limit in GiB, bounded by manifest and VM capacity")
	}
	return r
}
func (r *applianceResources) record(name string) {
	if _, ok := r.values[name]; ok {
		r.provided[name] = true
	}
}
func (r *applianceResources) apply(grant *appliancev1.ApprovedGrant, manifest *appliancev1.ControllerApplianceManifest) error {
	if r == nil {
		return nil
	}
	for _, scope := range []string{"system", "environment"} {
		var target, maximum *hostv1.ResourceLimits
		if scope == "system" {
			target, maximum = grant.GetSystemLimits(), manifest.GetSystemLimits()
		} else {
			target, maximum = grant.GetCapacity().GetPerEnvironmentLimits(), manifest.GetRequestedCapacity().GetPerEnvironmentLimits()
		}
		for _, unit := range []string{"pids", "disk-gib"} {
			name := scope + "-" + unit
			if !r.provided[name] {
				continue
			}
			value := *r.values[name]
			if target == nil || maximum == nil {
				return fmt.Errorf("--%s requires manifest resource limits", name)
			}
			if unit == "pids" {
				if value == 0 || value > math.MaxUint32 || value > uint64(maximum.GetPids()) {
					return fmt.Errorf("--%s must be positive and at most manifest limit %d", name, maximum.GetPids())
				}
				v := uint32(value)
				target.Pids = &v
			} else {
				if value == 0 || value > math.MaxUint64/(1<<30) {
					return fmt.Errorf("--%s must be a positive GiB value without overflow", name)
				}
				bytes := value * (1 << 30)
				if bytes > maximum.GetDiskBytes() {
					return fmt.Errorf("--%s exceeds manifest disk limit %d bytes", name, maximum.GetDiskBytes())
				}
				target.DiskBytes = &bytes
			}
		}
	}
	return nil
}
