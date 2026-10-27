//go:build darwin || linux

package host

import (
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/guestconn"
)

func TestContainerConfigurationObservationIsComputedOnlyWithExpectedDeclaration(t *testing.T) {
	expected := &hostv1.CreateContainerRequest{RequestId: "caller", ContainerId: "web", Image: "same-image", Env: []string{"CONFIG=first"}}
	fingerprint := containerConfigurationFingerprint(expected)
	actual := &vmmdv1.Container{Id: "web", ConfigurationFingerprint: &fingerprint}
	if result := guestconn.Container(actual); result.ConfigurationMatches != nil {
		t.Fatal("default/list projection invented configuration comparison")
	}
	if result := containerConfigurationObservation(actual, nil); result.ConfigurationMatches != nil {
		t.Fatal("query without declaration invented comparison")
	}
	if result := containerConfigurationObservation(actual, expected); result.ConfigurationMatches == nil || !result.GetConfigurationMatches() {
		t.Fatalf("matching declaration=%v", result)
	}
	expected.Env[0] = "CONFIG=second"
	if result := containerConfigurationObservation(actual, expected); result.ConfigurationMatches == nil || result.GetConfigurationMatches() {
		t.Fatalf("same-image config change=%v", result)
	}
	actual.ConfigurationFingerprint = nil
	if result := containerConfigurationObservation(actual, expected); result.ConfigurationMatches == nil || result.GetConfigurationMatches() {
		t.Fatalf("missing internal observation=%v", result)
	}
}
