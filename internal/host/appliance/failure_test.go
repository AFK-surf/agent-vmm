//go:build darwin || linux

package appliance

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/egress"
)

func TestApplianceFailureHelpersRevokeSourceBeforeCleanup(t *testing.T) {
	for _, action := range []string{"failed", "sealed", "invalid-instance", "failed-publication"} {
		t.Run(action, func(t *testing.T) {
			controller, installation, _, _, _, _ := renewalHost(t, time.Now())

			installation.RegistrationID = ""
			var err error
			installation, err = controller.store.UpdateApplianceInstallation(context.Background(), installation, installation.Revision)
			if err != nil {
				t.Fatal(err)
			}
			source := egress.Source{EnvironmentID: "appliance:" + installation.ID, NamespaceID: "system.test", Subnet: netip.MustParsePrefix("10.64.1.0/24"), LANEgress: true, InstanceID: "instance", GrantRevision: 1, HostTCPEndpoints: []netip.AddrPort{netip.MustParseAddrPort("192.168.1.10:3000")}}
			if err := controller.egress.Policies.Publish(egress.Snapshot{BootID: "boot", Revision: 1, Sources: []egress.Source{source}}); err != nil {
				t.Fatal(err)
			}
			flow, cancel := controller.egress.Policies.Context(context.Background(), "boot", source.Subnet.Addr().Next(), source)
			defer cancel()
			switch action {
			case "failed":
				_ = controller.failAppliance(context.Background(), installation, "TEST_FAILED", errors.New("test failure"))
			case "sealed":
				_ = controller.sealAppliance(context.Background(), installation, "TEST_SEALED")
			case "invalid-instance":
				_ = controller.invalidateApplianceRuntime(context.Background(), installation, nil, "TEST_INVALID")
			case "failed-publication":
				controller.revokeFailedAppliancePublication(context.Background(), installation.ID)
			}
			if _, allowed := controller.egress.Policies.Resolve("boot", source.Subnet.Addr().Next()); allowed {
				t.Fatal("invalidated installation retained egress admission")
			}
			select {
			case <-flow.Done():
			case <-time.After(time.Second):
				t.Fatal("invalidated installation retained its existing flow")
			}
		})
	}
}

// A transient publication failure, such as a bootstrap EOF while an adopted
// appliance starts, must not retire its managed authority: the appliance
// presents the same bundle again and an active authority accepts it.
func TestFailedPublicationKeepsManagedTrust(t *testing.T) {
	controller, installation, _, _, _, _ := renewalHost(t, time.Now())

	controller.revokeFailedAppliancePublication(context.Background(), installation.ID)
	if _, err := controller.store.GetApplianceMembershipCredential(context.Background(), installation.ID); err != nil {
		t.Fatalf("failed publication retired managed trust: %v", err)
	}
}
