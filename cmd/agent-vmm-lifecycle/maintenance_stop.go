package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// The operator confirmed interruption of the whole shared Host. Try the normal
// VM stop first. An older or damaged Host may not implement that RPC; launchd
// can still terminate its exact supervised process. Deletion requires process
// exit and release of the shared disk, regardless of RPC availability.
func stopForMaintenance(value paths) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	return stopForMaintenanceWith(ctx, value, func(ctx context.Context, args ...string) error { return launchctlContext(ctx, args...) })
}

func stopForMaintenanceWith(ctx context.Context, value paths, run func(context.Context, ...string) error) error {
	domains := []string{value.serviceDomain(), value.alternateServiceDomain()}
	var pids []int
	for _, domain := range domains {
		err := run(ctx, "print", domain+"/"+hostLabel)
		if errors.Is(err, errLaunchctlServiceNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("observe Host before maintenance: %w", err)
		}
		if domain == "system" && os.Geteuid() != 0 {
			return errors.New("administrator must stop the Host with agent-vmm-service-executor stop --job host, then retry this maintenance request")
		}
		pid, err := launchctlPID(ctx, domain+"/"+hostLabel)
		if err != nil {
			return fmt.Errorf("observe Host process before maintenance: %w", err)
		}
		if pid > 0 {
			pids = append(pids, pid)
		}
	}
	if len(pids) > 0 {
		graceful, cancel := context.WithTimeout(ctx, 40*time.Second)
		_ = drainVMForUpdate(graceful, value, newOperationID("maintenance-stop"))
		cancel()
	}
	for _, domain := range domains {
		if err := stopHostServiceAt(ctx, run, domain); err != nil {
			return fmt.Errorf("stop shared Host for maintenance: %w", err)
		}
	}
	for _, pid := range pids {
		if err := waitForProcessExit(ctx, pid); err != nil {
			return fmt.Errorf("Host process exit is not confirmed: %w", err)
		}
	}
	return waitForStateDiskRelease(ctx, filepath.Join(value.applicationData, "instance", "state.raw"))
}
