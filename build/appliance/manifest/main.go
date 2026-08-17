package main

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	output := flag.String("output", "", "output directory")
	imageDigest := flag.String("image-digest", "", "OCI image manifest digest")
	keyPath := flag.String("key", "", "Ed25519 publisher seed")
	release := flag.String("release", "", "appliance release")
	flag.Parse()
	if *output == "" || *imageDigest == "" || *keyPath == "" || *release == "" {
		flag.Usage()
		os.Exit(2)
	}
	seedText, err := os.ReadFile(*keyPath)
	check(err)
	seed, err := hex.DecodeString(strings.TrimSpace(string(seedText)))
	check(err)
	if len(seed) != ed25519.SeedSize {
		panic("invalid Ed25519 seed")
	}
	privateKey := ed25519.NewKeyFromSeed(seed)
	limits := func(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
	}
	manifest := &appliancev1.ControllerApplianceManifest{
		ContractVersion: "1", ProductId: "agent-vmm-mcp", PublisherKeyId: "agent-vmm-first-party", Release: *release,
		ImageReference: "agent-vmm.local/mcp@" + *imageDigest, ImageManifestDigest: *imageDigest, Platform: "linux/arm64",
		BootstrapProtocolVersion: "appliance-v1", ControllerProtocolVersion: "controller-ready-v1",
		SystemResources: &appliancev1.ResourceRange{Floor: limits(200, 128<<20, 64, 1<<30), Ceiling: limits(1000, 512<<20, 256, 2<<30)},
		DataVolumeBytes: 1 << 30,
		RequestedCapacity: &appliancev1.CapacityGrant{MaxEnvironments: 16,
			AggregateLimits: limits(8000, 16<<30, 4096, 64<<30), PerEnvironmentLimits: limits(2000, 4<<30, 1024, 16<<30),
			MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, MaxLeaseSeconds: 3600, DisconnectGraceSeconds: 30},
		RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		RequestedFrontends: []*appliancev1.ControllerFrontend{
			{Name: "mcp-http", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP,
				Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK,
				Bounds:   &appliancev1.StreamBounds{MaxFrameBytes: 1 << 20, MaxTotalBytes: 1 << 30, DeadlineSeconds: 3600, IdleSeconds: 300, MaxConcurrentStreams: 16, MaxUseCount: 1024}},
		},
		StartupDeadlineSeconds: 60, ProbationSeconds: 5, HealthTimeoutSeconds: 10, MinimumApplianceRuntime: "1",
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	check(err)
	digest := sha256.Sum256(canonical)
	check(os.MkdirAll(*output, 0o755))
	check(os.WriteFile(filepath.Join(*output, "manifest.pb"), canonical, 0o644))
	check(os.WriteFile(filepath.Join(*output, "manifest.sig"), ed25519.Sign(privateKey, canonical), 0o644))
	check(os.WriteFile(filepath.Join(*output, "publisher.pub"), privateKey.Public().(ed25519.PublicKey), 0o644))
	fmt.Println("sha256:" + hex.EncodeToString(digest[:]))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
