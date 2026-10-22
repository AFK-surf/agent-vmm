//go:build linux && guestsmoke

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/netprotocol"
	"github.com/AFK-surf/agent-vmm/internal/vmmprocess"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func main() {
	var err error
	if len(os.Args) > 1 && os.Args[1] == "__vmm" {
		err = vmmprocess.RunChild()
	} else {
		timer := time.AfterFunc(4*time.Minute, func() { fmt.Fprintln(os.Stderr, "Guest smoke exceeded deadline"); os.Exit(1) })
		defer timer.Stop()
		err = guestSmoke()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func guestSmoke() (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Second)
	defer cancel()
	directory, err := os.MkdirTemp("/tmp", "kvm-guest-smoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	state := filepath.Join(directory, "state.raw")
	f, err := os.Create(state)
	if err != nil {
		return err
	}
	err = f.Truncate(32 << 30)
	_ = f.Close()
	if err != nil {
		return err
	}
	config := vmmprocess.Config{KernelPath: "/work/Image", InitramfsPath: "/work/guest-initramfs.xz", RootFSPath: "/work/rootfs.squashfs", StatePath: state, InstanceDirectory: filepath.Join(directory, "instance"), SerialLog: "/work/guest-inner.log", CommandLine: "console=ttyS0 panic=-1 psi=1 agent.state_init=1", CPUCount: 1, MemoryBytes: 2 << 30, GuestManifestDigest: "cached-dependencies-current-vmmd-smoke"}
	client, err := vmmprocess.Start(ctx, config)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Stop()
		if result != nil {
			if data, err := os.ReadFile(config.SerialLog); err == nil {
				if len(data) > 12000 {
					data = data[len(data)-12000:]
				}
				fmt.Fprintf(os.Stderr, "Guest serial tail:\n%s\n", data)
			}
		}
	}()
	listener, err := client.Listen(netprotocol.EgressPort)
	if err != nil {
		return err
	}
	defer listener.Close()
	go serveGuestEgress(ctx, listener)
	connection, err := grpc.NewClient("passthrough:///guest-smoke", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return client.Dial(ctx, 4050) }))
	if err != nil {
		return err
	}
	defer connection.Close()
	api := vmmdv1.NewVMMDServiceClient(connection)
	var health *vmmdv1.HealthResponse
	for {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		health, err = api.Health(attempt, &vmmdv1.HealthRequest{})
		stop()
		if err == nil && health.GetReady() {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("wait VMMD health: %v: %w", err, ctx.Err())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !health.GetRootReadOnly() || !health.GetStateInitialized() || health.GetBootId() == "" {
		return fmt.Errorf("invalid VMMD health: %v", health)
	}
	fmt.Printf("PASS current VMMD ready, read-only root, partitioned state, boot=%s versions=%v\n", health.GetBootId(), health.GetRuntimeVersions())
	components, err := api.ComponentHealth(ctx, &vmmdv1.ComponentHealthRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("Guest component health: %v\n", components)
	for _, component := range components.GetComponents() {
		if component.GetState() != vmmdv1.ComponentState_COMPONENT_STATE_HEALTHY {
			return fmt.Errorf("Guest component %s: %s", component.GetComponent(), component.GetReason())
		}
	}

	usage, err := api.GetSystemUsage(ctx, &vmmdv1.GetSystemUsageRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("Guest system usage/time: %v\n", usage)
	guestTime := time.Unix(0, usage.GetUsage().GetSampledAtUnixNano())
	outerTime := time.Now()
	baselineData, err := os.ReadFile("/work/guest-smoke-host-unix-seconds")
	if err != nil {
		return err
	}
	baselineSeconds, err := strconv.ParseInt(strings.TrimSpace(string(baselineData)), 10, 64)
	if err != nil {
		return err
	}
	baseline := time.Unix(baselineSeconds, 0)
	for name, actual := range map[string]time.Time{"Guest": guestTime, "outer Linux": outerTime} {
		delta := actual.Sub(baseline)
		if delta < 0 {
			delta = -delta
		}
		if delta > 2*time.Minute {
			return fmt.Errorf("%s wall clock %s differs from macOS baseline %s", name, actual.UTC(), baseline.UTC())
		}
	}
	if delta := guestTime.Sub(outerTime); delta < -30*time.Second || delta > 30*time.Second {
		return fmt.Errorf("Guest clock differs from outer Linux by %s", delta)
	}
	fmt.Printf("PASS Guest RTC wall clock %s matches outer Linux %s and macOS baseline %s\n", guestTime.UTC().Format(time.RFC3339Nano), outerTime.UTC().Format(time.RFC3339Nano), baseline.UTC().Format(time.RFC3339))
	if err := guestWorkload(ctx, api); err != nil {
		return err
	}
	_, err = api.PrepareShutdown(ctx, &vmmdv1.PrepareShutdownRequest{RequestId: health.GetBootId()})
	if err != nil {
		return fmt.Errorf("prepare shutdown: %w", err)
	}
	for client.State() != hostv1.VMState_VM_STATE_STOPPED {
		if ctx.Err() != nil {
			return fmt.Errorf("wait Guest poweroff: %w", ctx.Err())
		}
		if client.State() == hostv1.VMState_VM_STATE_FAILED {
			return errors.New("Guest failed during poweroff")
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("PASS Guest PrepareShutdown and complete VMM exit")
	return nil
}

func serveGuestEgress(ctx context.Context, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			handshake, err := netprotocol.ReadHandshake(conn, "")
			if err != nil {
				return
			}
			if err := netprotocol.WriteHandshake(conn, handshake); err != nil {
				return
			}
			fmt.Println("PASS Guest dedicated egress handshake", handshake.BootID)
			for {
				kind, payload, err := netprotocol.ReadFrameContext(ctx, conn)
				if err != nil {
					return
				}
				if kind == netprotocol.Ping {
					if err := netprotocol.WriteFrame(conn, netprotocol.Pong, payload); err != nil {
						return
					}
				}
			}
		}()
	}
}

func guestWorkload(ctx context.Context, api vmmdv1.VMMDServiceClient) error {
	const namespace = "kvm-guest-smoke"
	_, err := api.EnsureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{RequestId: "smoke-namespace", NamespaceId: namespace, Limits: &vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 256 << 20, Pids: 64, DiskBytes: 1 << 30}, EgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL})
	if err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}
	image, err := os.ReadFile("/work/workload.oci.tar")
	if err != nil {
		return err
	}
	archiveHash := sha256.Sum256(image)
	archive, err := os.Open("/work/workload.oci.tar")
	if err != nil {
		return err
	}
	defer archive.Close()
	var index struct {
		Manifests []struct {
			Digest string `json:"digest"`
		} `json:"manifests"`
	}
	reader := tar.NewReader(archive)
	for {
		header, err := reader.Next()
		if err != nil {
			return err
		}
		if header.Name == "index.json" {
			if err := json.NewDecoder(reader).Decode(&index); err != nil {
				return err
			}
			break
		}
	}
	if len(index.Manifests) != 1 {
		return errors.New("invalid workload OCI index")
	}
	stream, err := api.ImportImage(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_Header{Header: &vmmdv1.ImportImageHeader{RequestId: "smoke-import", ActorNamespaceId: namespace, ImageReference: "agent.local/kvm-smoke:latest", ArchiveSha256: archiveHash[:], ManifestDigest: index.Manifests[0].Digest, Platform: "linux/arm64", ArchiveSize: uint64(len(image))}}}); err != nil {
		return err
	}
	for len(image) > 0 {
		n := min(len(image), vmmdv1.MaxImportArchiveChunkBytes)
		if err := stream.Send(&vmmdv1.ImportImageRequest{Payload: &vmmdv1.ImportImageRequest_ArchiveChunk{ArchiveChunk: image[:n]}}); err != nil {
			return err
		}
		image = image[n:]
	}
	imported, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("import image: %w", err)
	}
	fmt.Println("PASS current VMMD OCI import", imported.GetDigest())
	created, err := api.CreateContainer(ctx, &vmmdv1.CreateContainerRequest{RequestId: "smoke-create", ActorNamespaceId: namespace, ContainerId: "smoke", Image: "agent.local/kvm-smoke:latest", Limits: &vmmdv1.ResourceLimits{CpuMillis: proto.Uint32(1000), MemoryBytes: proto.Uint64(128 << 20), Pids: proto.Uint32(32)}, ReadOnlyRoot: true, LogLimitBytes: 4096, CommandOverride: &vmmdv1.StringList{Values: []string{"sh", "-c", "echo KVM_CURRENT_VMMD_WORKLOAD; /bin/busybox date +%s; /bin/busybox sleep 1"}}})
	if err != nil {
		return fmt.Errorf("create workload: %w", err)
	}
	_, err = api.StartContainer(ctx, &vmmdv1.StartContainerRequest{RequestId: "smoke-start", ActorNamespaceId: namespace, ContainerId: "smoke", ExpectedGenerationId: created.GetContainer().GetGenerationId()})
	if err != nil {
		return fmt.Errorf("start workload: %w", err)
	}
	waited, err := api.WaitContainer(ctx, &vmmdv1.WaitContainerRequest{ActorNamespaceId: namespace, ContainerId: "smoke"})
	if err != nil {
		return fmt.Errorf("wait workload: %w", err)
	}
	logs, err := api.Logs(ctx, &vmmdv1.LogsRequest{ActorNamespaceId: namespace, ContainerId: "smoke", TailRecords: 10})
	if err != nil {
		return err
	}
	var output bytes.Buffer
	for {
		record, err := logs.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("Guest workload log: %v\n", record)
		output.Write(record.GetData())
	}
	if waited.GetExitCode() != 0 {
		return fmt.Errorf("workload exit %d", waited.GetExitCode())
	}
	if !strings.Contains(output.String(), "KVM_CURRENT_VMMD_WORKLOAD\n") {
		return errors.New("Guest workload marker missing from logs")
	}
	fmt.Println("PASS current VMMD gVisor workload exited zero")
	return nil
}
