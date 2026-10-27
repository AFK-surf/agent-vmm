//go:build darwin || linux

package host

import (
	"context"
	"os"
	"os/exec"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVMStartDoesNotReenterLifecycleOwnerForPendingApplianceRemoval(t *testing.T) {
	if os.Getenv("AGENT_VMM_TEST_START_PENDING_REMOVE") != "1" {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestVMStartDoesNotReenterLifecycleOwnerForPendingApplianceRemoval$")
		command.Env = append(os.Environ(), "AGENT_VMM_TEST_START_PENDING_REMOVE=1", "TMPDIR="+t.TempDir())
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("VM Start subprocess: %v\n%s", err, output)
		}
		return
	}
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.SetRemoteEnrollment(ctx, registration.ID, registration.Revision, registration.EnrollmentState,
		int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL), "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "pending-remove", InstallationID: installation.ID,
		RequestID: "original-remove", Kind: 7, State: 1, Phase: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CreateApplianceOperation(ctx, store.ApplianceOperation{ID: "active-appliance", InstallationID: installation.ID,
		RequestID: "active-appliance", Kind: 3, State: 1, Phase: 2}); err != nil {
		t.Fatal(err)
	}
	controller.manager = &vmm.Manager{}

	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	defer hostLockWatchdog(t)()
	_, err = controller.vmLifecycle.StartVM(ctx, "start-with-pending-remove", &hostv1.VMMutationFence{})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("VM Start did not reach its own fence without consuming Remove: %v", err)
	}
	operation, err := controller.store.ApplianceOperationByRequest(ctx, "original-remove")
	if err != nil || operation.State != 1 || operation.Phase != 4 {
		t.Fatalf("VM Start consumed another operation's pending removal: %+v %v", operation, err)
	}
	operation, err = controller.store.ApplianceOperationByRequest(ctx, "active-appliance")
	if err != nil || operation.State != 1 || operation.Phase != 2 {
		t.Fatalf("VM Start classified an active appliance command as Host restart: %+v %v", operation, err)
	}
}
