//go:build darwin || linux

package host

import (
	"context"
	"path/filepath"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func updateDrainFixture(t *testing.T, frontends ...store.ApplianceFrontend) (*Controller, store.ApplianceInstallation) {
	t.Helper()
	return updateDrainFixtureAt(t, filepath.Join(t.TempDir(), "host.db"), frontends...)
}
func updateDrainFixtureAt(t *testing.T, path string, frontends ...store.ApplianceFrontend) (*Controller, store.ApplianceInstallation) {
	t.Helper()
	database, err := openTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	ctx := context.Background()
	manifestBytes, err := proto.Marshal(&appliancev1.ControllerApplianceManifest{SystemLimits: &hostv1.ResourceLimits{Pids: proto.Uint32(1), DiskBytes: proto.Uint64(1)}, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: &hostv1.ResourceLimits{Pids: proto.Uint32(1), DiskBytes: proto.Uint64(1)}, MaxEgressMode: 1}, RequestedEgressMode: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = database.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: "artifact", CanonicalHash: []byte("hash"), ProductID: "product", ImageDigest: "digest", ImageReference: "reference", Platform: "linux/arm64", Release: "1", Manifest: manifestBytes}, store.ApplianceOperation{ID: "stage", RequestID: "stage", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	installation, _, _, err := database.InstallApplianceDomain(ctx, store.ApplianceInstallation{ID: "installation", ProductID: "product", ArtifactID: "artifact", State: 4, Revision: 1, RegistrationID: "registration", DataVolumeID: "volume", DataVolumeBytes: 1024}, store.ApplianceGrant{Revision: 1, System: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, PerEnvironment: store.ResourceAmount{PIDs: 1, DiskBytes: 1}, EgressMode: 1, MaxEgressMode: 1}, store.RemoteRegistration{ID: "registration", OwnerKind: store.RemoteRegistrationPrivate, InstallationID: "installation", DisplayName: "private", PerEnvPIDs: 1, PerEnvDiskBytes: 1, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, EnrollmentState: 2, ConnectorPhase: 1}, frontends, store.ApplianceOperation{ID: "install", RequestID: "install", State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestControllerWithWorkloads(t, &Controller{store: database}, testWorkloadState{workloadRunsRecovered: true})
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	admitTestLifecycle(t, controller)
	return controller, installation
}
func seedApplianceUpdateWorkload(t *testing.T, controller *Controller, registrationID string) {
	t.Helper()
	setTestWorkloadState(t, controller, testWorkloadState{workloadRunsRecovered: true, workloadRuns: map[string]*testWorkloadRun{"workload": {registrationID: registrationID, instanceID: "workload-instance", executions: map[string]*testWorkloadExecution{"unknown": {}}}}})
}
