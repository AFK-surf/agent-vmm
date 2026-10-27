//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AFK-surf/agent-vmm/internal/host/appliance"
	"github.com/AFK-surf/agent-vmm/internal/host/testfixture"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestBundledArtifactFailureLeavesHostOperatorAvailable(t *testing.T) {
	root := t.TempDir()
	manager := removalRuntimeBundleManager(t, root)
	if err := os.WriteFile(filepath.Join(root, "bundle", "rootfs"), []byte("unavailable bundle"), 0600); err != nil {
		t.Fatal(err)
	}
	secrets, err := appliance.NewSecretStore(root)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := New(Config{Secrets: secrets, Credentials: remoteconnector.NewCredentialStore(filepath.Join(root, "remote-credentials")), TrustKeyProvider: testfixture.NewMemoryKeys(), Manager: manager, DatabasePath: filepath.Join(root, "host.db"), SessionDirectory: filepath.Join(root, "sessions"), InstanceDirectory: filepath.Join(root, "instance"), InstanceID: "test-host", CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30})
	if err != nil {
		t.Fatal("bundled artifact failure blocked Host", err)
	}
	t.Cleanup(func() { controller.Close() })
	client := removalOperatorClient(t, controller)
	if _, err := client.ListAppliances(context.Background(), &appliancev1.ListAppliancesRequest{}); err != nil {
		t.Fatal("operator unavailable", err)
	}
}
func removalRuntimeBundleManager(t *testing.T, root string) *vmm.Manager {
	t.Helper()
	directory := filepath.Join(root, "bundle")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	artifacts := make(map[string]bundle.Artifact)
	for _, name := range []string{"kernel", "initramfs", "rootfs"} {
		data := []byte(name)
		hash := sha256.Sum256(data)
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		artifacts[name] = bundle.Artifact{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	}
	manifest, err := json.Marshal(bundle.Manifest{SchemaVersion: 1, Architecture: "arm64", Artifacts: artifacts,
		RootFS: bundle.RootFS{Filesystem: "squashfs", Compressor: "zstd", BlockSize: 1 << 20, Filter: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, bundle.ManifestName), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(manifest)
	manager, err := vmm.NewManager(vmm.Config{BundleDirectory: directory, InstanceDirectory: filepath.Join(root, "instance"),
		SerialLog: filepath.Join(root, "serial.log"), CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30, GuestManifestDigest: hex.EncodeToString(hash[:])})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}
func removalOperatorClient(t *testing.T, controller *Controller) appliancev1.ApplianceAdminServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	operator := controller.NewOperatorServer()
	go operator.Serve(listener)
	t.Cleanup(operator.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	client := appliancev1.NewApplianceAdminServiceClient(connection)
	return client
}
func TestPendingApplianceRemovalDoesNotGateHostConstruction(t *testing.T) {
	root := t.TempDir()
	databasePath := filepath.Join(root, "host.db")
	seed, installation := updateDrainFixtureAt(t, databasePath)
	_, err := seed.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{ID: "pending-remove", InstallationID: installation.ID, RequestID: "original-remove", Kind: 7, State: 1, Phase: 4, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), Receipt: []byte("original-pending-receipt")})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.store.Close(); err != nil {
		t.Fatal(err)
	}
	manager := removalRuntimeBundleManager(t, root)
	secrets, err := appliance.NewSecretStore(root)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := New(Config{Secrets: secrets, Credentials: remoteconnector.NewCredentialStore(filepath.Join(root, "remote-credentials")), TrustKeyProvider: testfixture.NewMemoryKeys(), Manager: manager, DatabasePath: databasePath, SessionDirectory: filepath.Join(root, "sessions"), InstanceDirectory: filepath.Join(root, "instance"), InstanceID: "test-host", CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { controller.Close() })
	client := removalOperatorClient(t, controller)
	response, err := client.ListAppliances(context.Background(), &appliancev1.ListAppliancesRequest{})
	if err != nil || len(response.GetAppliances()) != 1 {
		t.Fatalf("pending removal blocked Operator: %v %v", response, err)
	}
}
