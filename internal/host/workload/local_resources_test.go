//go:build darwin || linux

package workload

import (
	"context"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

type localResourceVM struct{ vm *hostv1.VM }

func (manager localResourceVM) Snapshot() *hostv1.VM {
	return proto.Clone(manager.vm).(*hostv1.VM)
}

type localResourceGuest struct {
	*workloadGuest
	usage      *vmmdv1.Usage
	afterUsage func()
}

func (guest localResourceGuest) GetNamespaceUsage(context.Context, *vmmdv1.GetNamespaceUsageRequest) (*vmmdv1.GetUsageResponse, error) {
	if guest.afterUsage != nil {
		guest.afterUsage()
	}
	return &vmmdv1.GetUsageResponse{Usage: guest.usage}, nil
}

func TestWorkloadSamplingPopulatesLocalResourceSummary(t *testing.T) {
	controller := workloadTestController(t)
	if _, err := controller.store.CreateRemoteRegistration(context.Background(), observationTestRegistration("registration")); err != nil {
		t.Fatal(err)
	}
	environment, err := controller.store.CreateEnvironment(context.Background(), observationTestEnvironment("environment", "registration"))
	if err != nil {
		t.Fatal(err)
	}
	vm := &hostv1.VM{BootId: "boot", State: hostv1.VMState_VM_STATE_RUNNING}
	controller.manager = localResourceVM{vm}
	run := &Run{environmentID: environment.ID, namespaceID: environment.NamespaceID, containerID: "main", instanceID: "instance"}
	controller.setWorkloadRunLocked(workloadRunKey(run.environmentID, run.containerID), run)
	sampledAt := time.Now().Add(-time.Second)
	guest := localResourceGuest{
		workloadGuest: &workloadGuest{states: map[string]*vmmdv1.Container{
			environment.NamespaceID: {Id: run.containerID, InstanceId: run.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING},
		}},
		usage: &vmmdv1.Usage{DiskBytes: 123, SampledAtUnixNano: sampledAt.UnixNano()},
	}
	controller.sampleWorkloadRuns(context.Background(), workloadGuestClient(t, guest))

	item := &hostv1.LocalEnvironmentSummary{State: "unknown"}
	controller.ApplyLocalResourceSample(item, environment, vm, time.Now())
	if item.PrivateDiskBytes == nil || item.GetPrivateDiskBytes() != 123 || item.GetState() != "observed_active" || !item.GetSampledAt().AsTime().Equal(sampledAt) {
		t.Fatalf("background sample did not reach local summary: %v", item)
	}
	for _, scope := range []string{"old sample", "new boot", "new namespace", "new generation", "stopped VM"} {
		t.Run(scope, func(t *testing.T) {
			row, machine, now := environment, proto.Clone(vm).(*hostv1.VM), time.Now()
			switch scope {
			case "old sample":
				now = sampledAt.Add(localResourceFreshness + time.Nanosecond)
			case "new boot":
				machine.BootId = "replacement"
			case "new namespace":
				row.NamespaceID = "replacement"
			case "new generation":
				row.AllocationGeneration++
			case "stopped VM":
				machine.State = hostv1.VMState_VM_STATE_STOPPED
			}
			item := &hostv1.LocalEnvironmentSummary{State: "unknown"}
			controller.ApplyLocalResourceSample(item, row, machine, now)
			if item.PrivateDiskBytes != nil || item.GetSampledAt() != nil || item.GetState() != "unknown" {
				t.Fatalf("sample crossed %s: %v", scope, item)
			}
		})
	}
}

func TestWorkloadSamplingDoesNotPublishAcrossReplacement(t *testing.T) {
	for _, replacement := range []string{"run", "instance", "boot"} {
		t.Run(replacement, func(t *testing.T) {
			controller := workloadTestController(t)
			if _, err := controller.store.CreateRemoteRegistration(context.Background(), observationTestRegistration("registration")); err != nil {
				t.Fatal(err)
			}
			environment, err := controller.store.CreateEnvironment(context.Background(), observationTestEnvironment("environment", "registration"))
			if err != nil {
				t.Fatal(err)
			}
			vm := &hostv1.VM{BootId: "boot", State: hostv1.VMState_VM_STATE_RUNNING}
			controller.manager = localResourceVM{vm}
			run := &Run{environmentID: environment.ID, namespaceID: environment.NamespaceID, containerID: "main", instanceID: "instance"}
			key := workloadRunKey(run.environmentID, run.containerID)
			controller.setWorkloadRunLocked(key, run)
			guest := localResourceGuest{
				workloadGuest: &workloadGuest{states: map[string]*vmmdv1.Container{
					environment.NamespaceID: {Id: run.containerID, InstanceId: run.instanceID, State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING},
				}},
				usage: &vmmdv1.Usage{DiskBytes: 123, SampledAtUnixNano: time.Now().UnixNano()},
				afterUsage: func() {
					controller.mu.Lock()
					defer controller.mu.Unlock()
					switch replacement {
					case "run":
						controller.setWorkloadRunLocked(key, &Run{environmentID: run.environmentID, namespaceID: run.namespaceID, containerID: run.containerID, instanceID: "replacement"})
					case "instance":
						run.instanceID = "replacement"
					case "boot":
						vm.BootId = "replacement"
					}
				},
			}
			controller.sampleWorkloadRuns(context.Background(), workloadGuestClient(t, guest))
			item := &hostv1.LocalEnvironmentSummary{State: "unknown"}
			controller.ApplyLocalResourceSample(item, environment, vm, time.Now())
			if item.PrivateDiskBytes != nil || item.GetState() != "unknown" {
				t.Fatalf("replacement %s inherited a local resource sample: %v", replacement, item)
			}
		})
	}
}
