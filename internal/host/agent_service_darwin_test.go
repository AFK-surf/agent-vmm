//go:build darwin

package host

import (
	"testing"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/host/store"
)

func TestSameServiceDomain(t *testing.T) {
	if !sameServiceDomain("environment-a", "registration-a", &store.Environment{ID: "environment-a", RegistrationID: ""}) {
		t.Fatal("same environment was rejected")
	}
	if !sameServiceDomain("environment-a", "registration-a", &store.Environment{ID: "environment-b", RegistrationID: "registration-a"}) {
		t.Fatal("same registration domain was rejected")
	}
	if sameServiceDomain("environment-a", "registration-a", &store.Environment{ID: "environment-b", RegistrationID: "registration-b"}) {
		t.Fatal("different registration domain was accepted")
	}
}

func TestLocalServiceRouteCapabilityIsBounded(t *testing.T) {
	capability := localServiceRouteCapability("request", "device", "allocation", "export", false)
	if capability.GetSourceDeviceId() != "device" || capability.GetDestinationDeviceId() != "device" || capability.GetDestinationExportId() != "export" || capability.GetSourceAllocationId() != "allocation" {
		t.Fatalf("capability binding = %v", capability)
	}
	if capability.GetRouteGeneration() == 0 || capability.GetPolicyRevision() != 1 || capability.GetExpiresAt() == nil || !capability.GetExpiresAt().AsTime().After(time.Now()) {
		t.Fatalf("capability lease = %v", capability)
	}
}
