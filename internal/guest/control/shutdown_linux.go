//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

const defaultContainerStopTimeout = 10 * time.Second

func (s *Server) stopWriters(ctx context.Context) error {
	s.runtime.stopRuntimeGC()
	s.runtime.stopDiskSampler()
	err := errors.Join(s.settleImageOperations(ctx), s.appliances.shutdown(ctx), s.runtime.stopWorkloadTasks(ctx), s.runtime.cleanupTasks(ctx))
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() { s.runtime.tasks.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) stopWorkloadTasks(ctx context.Context) error {
	containers, err := r.client.Containers(runtimeContext(ctx))
	if err != nil {
		return err
	}
	var failures []error
	for _, container := range containers {
		info, err := container.Info(runtimeContext(ctx))
		if err != nil {
			failures = append(failures, err)
			continue
		}
		namespace, id := info.Labels[runtimeOwnerLabel], info.Labels[runtimeExternalIDLabel]
		if namespace == "" {
			continue
		}
		current, err := r.stopContainer(ctx, namespace, id, defaultContainerStopTimeout, isSystemNamespace(namespace))
		if err == nil && current.GetState() == vmmdv1.ContainerState_CONTAINER_STATE_UNKNOWN {
			err = errors.New("sandbox teardown is unresolved")
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("stop workload %s/%s: %w", namespace, id, err))
		}
	}
	return errors.Join(failures...)
}

func (r *Runtime) closeImageOperationAdmission() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.imageOperationsClosing = true
	r.imageOperations.Range(func(_, value any) bool { value.(*imageOperationRun).cancel(); return true })
}

func (s *Server) settleImageOperations(ctx context.Context) error {
	var failures []error
	s.runtime.imageOperations.Range(func(key, value any) bool {
		run := value.(*imageOperationRun)
		select {
		case <-run.done:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
			return false
		}
		var err error
		if run.request == nil {
			err = run.cleanup(ctx)
			if err == nil {
				s.runtime.imageOperations.Delete(key)
			}
		} else {
			_, err = s.ObserveImageOperation(ctx, run.request)
		}
		if err != nil {
			failures = append(failures, err)
		}
		return true
	})
	return errors.Join(failures...)
}

func (supervisor *applianceSupervisor) shutdown(ctx context.Context) error {
	supervisor.mu.Lock()
	records := make([]*systemApplianceRecord, 0, len(supervisor.records))
	for _, record := range supervisor.records {
		record.stopping = true
		records = append(records, record)
	}
	supervisor.mu.Unlock()
	var failures []error
	for _, record := range records {
		record.cleanupMu.Lock()
		supervisor.mu.Lock()
		current := supervisor.records[record.status.GetIdentity().GetInstallationId()] == record
		supervisor.mu.Unlock()
		if current {
			if _, err := supervisor.stopRecord(ctx, record, defaultContainerStopTimeout); err != nil {
				failures = append(failures, err)
			}
		}
		record.cleanupMu.Unlock()
	}
	return errors.Join(failures...)
}
