//go:build darwin || linux

package servicenet

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
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
	capability := LocalServiceRouteCapability("request", "device", "allocation", "export", false)
	if capability.GetSourceDeviceId() != "device" || capability.GetDestinationDeviceId() != "device" || capability.GetDestinationExportId() != "export" || capability.GetSourceAllocationId() != "allocation" {
		t.Fatalf("capability binding = %v", capability)
	}
	if capability.GetRouteGeneration() == 0 || capability.GetPolicyRevision() != 1 || capability.GetExpiresAt() == nil || !capability.GetExpiresAt().AsTime().After(time.Now()) {
		t.Fatalf("capability lease = %v", capability)
	}
}

func TestHighBitLocalServiceGenerationPersistsExactRouteAndLease(t *testing.T) {
	for _, sameEnvironment := range []bool{true, false} {
		t.Run(map[bool]string{true: "same-environment", false: "allocation-route"}[sameEnvironment], func(t *testing.T) {
			database, err := store.Open(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			ctx := context.Background()
			for _, id := range []string{"source", "destination"} {
				_, err := database.CreateEnvironment(ctx, store.Environment{ID: id, NamespaceID: id, DisplayName: id, PIDs: 1, DiskBytes: 1, EgressMode: 1, OwnerKind: store.OwnerLocal})
				if err != nil {
					t.Fatal(err)
				}
			}
			destination := "destination"
			class := servicev1.RouteClass_ROUTE_CLASS_COMPUTE
			if sameEnvironment {
				destination = "source"
				class = servicev1.RouteClass_ROUTE_CLASS_LOCAL
			}
			_, err = database.CreateServiceExport(ctx, "export-request", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: destination, ContainerId: "server", ContainerInstanceId: "server-instance", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP})
			if err != nil {
				t.Fatal(err)
			}
			requestID := "service-import-1"
			original := sha256.Sum256([]byte(requestID))
			if binary.BigEndian.Uint64(original[:8]) <= 1<<63-1 {
				t.Fatal("fixture does not exercise the high-bit generation")
			}
			capability := LocalServiceRouteCapability(requestID, "device", "allocation", "export", sameEnvironment)
			generation := capability.GetRouteGeneration()
			if generation == 0 || generation > 1<<63-1 {
				t.Fatalf("generation %d cannot be persisted", generation)
			}
			route := &servicev1.AccessRoute{RouteId: "route", RouteClass: class, SourceDeviceId: "device", SourceAllocationId: proto.String("allocation"), DestinationDeviceId: "device", DestinationExportId: "export", State: servicev1.RouteState_ROUTE_STATE_ACTIVE, Revision: 1, Generation: generation, ExpiresAt: capability.GetExpiresAt(), SourceEnvironmentId: "source", SourceContainerInstanceId: "client-instance", PolicyRevision: 1}
			lease := &servicev1.RouteLease{LeaseId: "lease", RouteId: "route", RouteRevision: 1, Generation: generation, ExpiresAt: capability.GetExpiresAt(), Capability: capability}
			value := &servicev1.ServiceImport{ImportId: "import", SourceEnvironmentId: "source", SourceContainerId: "client", SourceContainerInstanceId: "client-instance", DestinationDeviceId: "device", DestinationExportId: "export", VirtualServiceName: "database", DestinationPort: 8080}
			created, err := database.CreateServiceImport(ctx, requestID, value, route, lease)
			if err != nil {
				t.Fatal(err)
			}
			if created.GetDestinationPort() != 8080 {
				t.Fatal("import projection lost the service port")
			}
			record, err := database.GetServiceRoute(ctx, "route", "lease", generation)
			if err != nil {
				t.Fatal(err)
			}
			if record.Route.GetGeneration() != generation || record.Route.GetRouteClass() != class || record.Route.GetSourceAllocationId() != "allocation" || !proto.Equal(record.Capability, capability) {
				t.Fatalf("route authority changed: %+v", record)
			}
			replayed, err := database.CreateServiceImport(ctx, requestID, value, route, lease)
			if err != nil || replayed.GetImportId() != created.GetImportId() {
				t.Fatalf("request replay: %+v %v", replayed, err)
			}
			if _, err := database.GetServiceRoute(ctx, "route", "lease", generation+1); err == nil {
				t.Fatal("wrong generation bypassed the route fence")
			}
			if again := LocalServiceRouteCapability(requestID, "device", "allocation", "export", sameEnvironment); again.GetRouteGeneration() != generation {
				t.Fatal("same request changed generation")
			}
		})
	}
}
