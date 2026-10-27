//go:build darwin && e2e

package e2e

import (
	"sync/atomic"
	"testing"
)

func TestE2E(t *testing.T) {
	t.Run("boot", testBoot)

	harness := startHostWithState(t, 32<<30)
	harness.start(t)
	t.Run("system_appliance", func(t *testing.T) { testSystemAppliance(t, harness) })
	client := harness.runtime(t)
	t.Run("session_auth_and_revocation", func(t *testing.T) { testSessionAuthAndRevocation(t, harness, client) })
	t.Run("remote_workflow", func(t *testing.T) { testRemoteWorkflow(t, harness) })
	t.Run("six_remote_domains", func(t *testing.T) { testSixRemoteDomains(t, harness) })
	t.Run("mcp_workflow", func(t *testing.T) { testMCPWorkflow(t, harness) })
	t.Run("workspace_file_workflow", func(t *testing.T) { testWorkspaceFileWorkflow(t, client) })
	t.Run("image_record_lifecycle", func(t *testing.T) { testImageRecordLifecycle(t, client) })
	parallel := []struct {
		name string
		run  func(*testing.T, *productClient)
	}{
		{"container_lifecycle", testContainerLifecycle},
		{"named_volume", testNamedVolume},
		{"gofer_file_cache", testGoferFileCache},
		{"disk_quota", testDiskQuota},
		{"same_namespace", testSameNamespace},
		{"default_isolation", testDefaultIsolation},
		{"service_route_scope", func(t *testing.T, _ *productClient) { testServiceRouteScope(t, harness) }},
		{"service_route_revoke", func(t *testing.T, _ *productClient) { testServiceRouteRevoke(t, harness) }},
		{"port_forward", testPortForward},
	}
	limit := make(chan struct{}, 4)
	var active, peak atomic.Int32
	t.Run("parallel", func(t *testing.T) {
		for _, test := range parallel {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				limit <- struct{}{}
				current := active.Add(1)
				for previous := peak.Load(); current > previous && !peak.CompareAndSwap(previous, current); previous = peak.Load() {
				}
				defer func() { active.Add(-1); <-limit }()
				test.run(t, client)
			})
		}
	})
	t.Logf("E2E_PARALLEL_PEAK=%d", peak.Load())
	// Bulk OCI reservations can legitimately reject unrelated namespace admission.
	// Each build scenario admits its namespaces before starting its workers.
	t.Run("dockerfile_build", func(t *testing.T) { testDockerfileBuild(t, client) })
	t.Run("parallel_agents", func(t *testing.T) { testParallelAgents(t, client) })
	t.Run("cancellation_cleanup", func(t *testing.T) { testCancellationCleanup(t, client) })
	t.Run("egress", func(t *testing.T) { testEgress(t, client) })
	t.Run("imported_image_forced_restart", func(t *testing.T) { testImportedImageForcedRestart(t, harness) })
	t.Run("provider_state_VM_restart", func(t *testing.T) { testProviderStateVMRestart(t, harness) })
	t.Run("VM_restart", func(t *testing.T) { testVMRestart(t, harness) })
	t.Run("forced_VM_stop", func(t *testing.T) { testForcedVMStop(t, harness) })
	t.Run("cleanup_contract", func(t *testing.T) { testCleanupContract(t, harness, client) })
	t.Run("residual_scan", func(t *testing.T) { testResidualScan(t, harness.runtime(t)) })
	harness.stop(t, false)
	t.Run("pgvector_image_and_volume", func(t *testing.T) {
		// A failed registry download can intentionally leave an unknown runtime
		// outcome. Keep this network-dependent contract on a dedicated final VM so
		// it cannot cascade projection drift into the local lifecycle matrix.
		isolated := startHostWithState(t, 32<<30)
		isolated.start(t)
		t.Cleanup(func() { isolated.stop(t, true) })
		testPGVectorImageAndVolume(t, isolated.runtime(t))
	})
}
