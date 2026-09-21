//go:build darwin && !managedonly

package main

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const maxContextBytes = 256 << 20

func main() {
	if len(os.Args) >= 2 && isManagedCommand(os.Args[1]) {
		os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
	}
	if len(os.Args) >= 3 && os.Args[1] == "appliance" && os.Args[2] == "build" {
		build(os.Args[3:])
		return
	}
	if len(os.Args) >= 3 && os.Args[1] == "appliance" && os.Args[2] == "status" {
		applianceStatus(os.Args[3:])
		return
	}
	fmt.Fprintln(os.Stderr, managedUsage)
	os.Exit(2)
}

func applianceStatus(arguments []string) {
	flags := flag.NewFlagSet("appliance status", flag.ExitOnError)
	socket := flags.String("operator-socket", filepath.Join(os.Getenv("HOME"), "Library/Application Support/AgentVMM/operator.sock"), "Agent VMM operator socket")
	flags.Parse(arguments)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}))
	check(err)
	defer connection.Close()
	response, err := appliancev1.NewApplianceAdminServiceClient(connection).List(ctx, &appliancev1.ListAppliancesRequest{})
	check(err)
	for _, appliance := range response.GetAppliances() {
		health := appliance.GetHealth()
		fmt.Printf("id=%s state=%s channel=%s enabled=%t healthy=%t status=%s boot=%s container=%s\n", appliance.GetInstallationId(), appliance.GetApplianceState(), appliance.GetChannelState(), appliance.GetEnabled(), health.GetHealthy(), health.GetStatus(), appliance.GetBootId(), appliance.GetContainerInstance())
		for _, frontend := range appliance.GetFrontends() {
			fmt.Printf("  frontend=%s endpoint=%s available=%t\n", frontend.GetName(), frontend.GetEndpoint(), frontend.GetAvailable())
		}
	}
}

func build(arguments []string) {
	flags := flag.NewFlagSet("appliance build", flag.ExitOnError)
	contextDirectory := flags.String("context", "", "Dockerfile build context")
	dockerfile := flags.String("dockerfile", "Dockerfile", "Dockerfile path relative to context")
	output := flags.String("output", "", "artifact directory")
	product := flags.String("product", "", "appliance product id")
	release := flags.String("release", "", "appliance release")
	socket := flags.String("operator-socket", filepath.Join(os.Getenv("HOME"), "Library/Application Support/AgentVMM/operator.sock"), "Agent VMM operator socket")
	artifactStore := flags.String("artifact-store", filepath.Join(os.Getenv("HOME"), "Library/Application Support/AgentVMM/instance/support/appliance-artifacts"), "Agent VMM appliance archive store")
	flags.Parse(arguments)
	if *contextDirectory == "" || *output == "" || *product == "" || *release == "" {
		flags.Usage()
		os.Exit(2)
	}
	archive, err := archiveContext(*contextDirectory)
	check(err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}))
	check(err)
	defer connection.Close()
	stream, err := appliancev1.NewApplianceAdminServiceClient(connection).BuildArtifact(ctx)
	check(err)
	requestID := randomID()
	check(stream.Send(&appliancev1.BuildArtifactRequest{Event: &appliancev1.BuildArtifactRequest_Header{Header: &appliancev1.BuildArtifactHeader{RequestId: requestID, DockerfilePath: filepath.ToSlash(*dockerfile), NetworkMode: hostv1.BuildNetworkMode_BUILD_NETWORK_MODE_NAMESPACE_EGRESS}}}))
	for len(archive) != 0 {
		count := min(len(archive), 1<<20)
		final := count == len(archive)
		check(stream.Send(&appliancev1.BuildArtifactRequest{Event: &appliancev1.BuildArtifactRequest_Context{Context: &hostv1.BuildContextChunk{Data: archive[:count], Final: final}}}))
		archive = archive[count:]
	}
	check(stream.CloseSend())
	check(os.MkdirAll(*output, 0o755))
	temporary, err := os.CreateTemp(*output, ".image.oci-*.tar")
	check(err)
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	var result *appliancev1.BuildArtifactResult
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		check(err)
		switch event := response.GetEvent().(type) {
		case *appliancev1.BuildArtifactResponse_Progress:
			fmt.Fprintln(os.Stderr, event.Progress)
		case *appliancev1.BuildArtifactResponse_ArchiveChunk:
			_, err = temporary.Write(event.ArchiveChunk)
			check(err)
		case *appliancev1.BuildArtifactResponse_Result:
			result = event.Result
		}
	}
	check(temporary.Close())
	if result == nil {
		check(errors.New("build returned no artifact result"))
	}
	image, err := os.ReadFile(temporaryPath)
	check(err)
	hash := sha256.Sum256(image)
	if uint64(len(image)) != result.GetArchiveSize() || !strings.EqualFold(hex.EncodeToString(hash[:]), hex.EncodeToString(result.GetArchiveSha256())) {
		check(errors.New("build archive does not match guest commitment"))
	}
	check(os.Rename(temporaryPath, filepath.Join(*output, "image.oci.tar")))
	canonicalHash := writeManifest(*output, *product, *release, result.GetManifestDigest())
	check(os.MkdirAll(*artifactStore, 0o700))
	storePath := filepath.Join(*artifactStore, hex.EncodeToString(canonicalHash)+".oci.tar")
	check(copyFile(filepath.Join(*output, "image.oci.tar"), storePath))
}

func archiveContext(root string) ([]byte, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	output := new(limitedBuffer)
	writer := tar.NewWriter(output)
	err = filepath.WalkDir(root, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == root {
			return nil
		}
		relative, err := filepath.Rel(root, name)
		if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
			return errors.New("invalid context path")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(relative)
		if info.Mode()&os.ModeSymlink != 0 {
			header.Linkname, err = os.Readlink(name)
			if err != nil {
				return err
			}
		}
		if info.Mode().IsRegular() && info.Size() > maxContextBytes {
			return errors.New("context file exceeds size limit")
		}
		if err := writer.WriteHeader(header); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			file, err := os.Open(name)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(writer, file)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
		return nil
	})
	if err == nil {
		err = writer.Close()
	}
	if err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

type limitedBuffer struct{ strings.Builder }

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	if buffer.Len()+len(data) > maxContextBytes {
		return 0, errors.New("context archive exceeds size limit")
	}
	return buffer.Builder.Write(data)
}

func (buffer *limitedBuffer) Bytes() []byte { return []byte(buffer.String()) }

func randomID() string {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value)
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}

func copyFile(source, destination string) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".appliance-*.oci.tar")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := io.Copy(temporary, input); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, destination)
}

func writeManifest(output, product, release, imageDigest string) []byte {
	limits := func(pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk}
	}
	manifest := &appliancev1.ControllerApplianceManifest{ContractVersion: "1", ProductId: product, Release: release, ImageReference: "agent-vmm.local/appliance@" + imageDigest, ImageManifestDigest: imageDigest, Platform: "linux/arm64", BootstrapProtocolVersion: "appliance-v1", ControllerProtocolVersion: "controller-ready-v1", SystemLimits: limits(256, 2<<30), DataVolumeBytes: 1 << 30, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: limits(1024, 16<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}, RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, RequestedFrontends: []*appliancev1.ControllerFrontend{{Name: "mcp-http", Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP, Exposure: appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1 << 20, MaxTotalBytes: 1 << 30, DeadlineSeconds: 3600, IdleSeconds: 300, MaxConcurrentStreams: 16, MaxUseCount: 1024}}}, StartupDeadlineSeconds: 60, ProbationSeconds: 5, HealthTimeoutSeconds: 10}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	check(err)
	check(os.WriteFile(filepath.Join(output, "manifest.pb"), canonical, 0o644))
	hash := sha256.Sum256(canonical)
	return hash[:]
}
