package main

import (
	"flag"
	"os"
	"path/filepath"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	output := flag.String("output", "", "output directory")
	imageDigest := flag.String("image-digest", "", "OCI image manifest digest")
	release := flag.String("release", "", "appliance release")
	flag.Parse()
	if *output == "" || *imageDigest == "" || *release == "" {
		flag.Usage()
		os.Exit(2)
	}
	limits := func(pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk}
	}
	manifest := &appliancev1.ControllerApplianceManifest{
		ContractVersion: "1", ProductId: "agent-vmm-mcp", Release: *release,
		ImageReference: "agent-vmm.local/mcp@" + *imageDigest, ImageManifestDigest: *imageDigest, Platform: "linux/arm64",
		SystemLimits:    limits(256, 2<<30),
		DataVolumeBytes: 1 << 30,
		RequestedCapacity: &appliancev1.CapacityGrant{
			PerEnvironmentLimits: limits(1024, 16<<30),
			MaxEgressMode:        hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET},
		RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
		RequestedFrontends: []*appliancev1.ControllerFrontend{
			{Name: "mcp-http", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP,
				Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK,
				Bounds:   &appliancev1.StreamBounds{MaxFrameBytes: 1 << 20, MaxTotalBytes: 1 << 30, DeadlineSeconds: 3600, IdleSeconds: 300, MaxConcurrentStreams: 16}},
		},
		StartupDeadlineSeconds: 60,
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	check(err)
	check(os.MkdirAll(*output, 0o755))
	check(os.WriteFile(filepath.Join(*output, "manifest.pb"), canonical, 0o644))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
