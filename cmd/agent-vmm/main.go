//go:build (darwin || linux) && !managedonly

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if len(os.Args) > 1 && os.Args[1] == "mcp" {
		os.Exit(runMCP(os.Args[2:]))
	}
	if runPlatformMode() {
		return
	}
	if len(os.Args) >= 2 && isManagedCommand(os.Args[1]) {
		os.Exit(runManagedCLI(os.Args[1:], os.Stdout, os.Stderr, managedRuntime{}))
	}
	if len(os.Args) >= 3 && os.Args[1] == "appliance" && os.Args[2] == "build" {
		build(os.Args[3:])
		return
	}
	if dispatchApplianceCLI(os.Args[1:]) {
		return
	}
	fmt.Fprintln(os.Stderr, managedUsage)
	os.Exit(2)
}

func build(arguments []string) {
	flags := flag.NewFlagSet("appliance build", flag.ExitOnError)
	contextDirectory := flags.String("context", "", "Dockerfile build context")
	dockerfile := flags.String("dockerfile", "Dockerfile", "Dockerfile path relative to context")
	output := flags.String("output", "", "artifact directory")
	product := flags.String("product", "", "appliance product id")
	release := flags.String("release", "", "appliance release")
	socket := flags.String("operator-socket", filepath.Join(platformDataDirectory(os.Getenv("HOME")), "operator.sock"), "Agent VMM operator socket")
	var frontends []string
	flags.Func("frontend", "HTTP frontend NAME=loopback|outbound-only|lan-http (repeatable)", func(value string) error { frontends = append(frontends, value); return nil })
	flags.Parse(arguments)
	if *contextDirectory == "" || *output == "" || *product == "" || *release == "" {
		flags.Usage()
		os.Exit(2)
	}
	declaredFrontends, err := parseApplianceFrontends(frontends)
	check(err)
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
	image, err := os.Open(temporaryPath)
	check(err)
	hash := sha256.New()
	size, err := io.Copy(hash, image)
	check(err)
	check(image.Close())
	if uint64(size) != result.GetArchiveSize() || !bytes.Equal(hash.Sum(nil), result.GetArchiveSha256()) {
		check(errors.New("build archive does not match guest commitment"))
	}
	check(os.Rename(temporaryPath, filepath.Join(*output, "image.oci.tar")))
	writeManifest(*output, *product, *release, &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ArchiveSha256: result.GetArchiveSha256(), ArchiveSize: result.GetArchiveSize(), Platform: "linux/arm64", ImageReference: "agent-vmm.local/appliance@" + result.GetManifestDigest(), ImageManifestDigest: result.GetManifestDigest()}, declaredFrontends...)

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
func writeManifest(output, product, release string, content *appliancev1.ApplianceContentReference, frontends ...*appliancev1.ControllerFrontend) []byte {
	limits := func(pids uint32, disk uint64) *hostv1.ResourceLimits {
		return &hostv1.ResourceLimits{Pids: &pids, DiskBytes: &disk}
	}
	manifest := &appliancev1.ControllerApplianceManifest{ContractVersion: "1", ProductId: product, Release: release, ImageReference: content.GetImageReference(), ImageManifestDigest: content.GetImageManifestDigest(), ContentRefs: []*appliancev1.ApplianceContentReference{content}, Platform: "linux/arm64", SystemLimits: limits(256, 2<<30), DataVolumeBytes: 1 << 30, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: limits(1024, 16<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET}, RequestedEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, RequestedFrontends: frontends, StartupDeadlineSeconds: 60}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(manifest)
	check(err)
	check(os.WriteFile(filepath.Join(output, "manifest.pb"), canonical, 0o644))
	hash := sha256.Sum256(canonical)
	return hash[:]
}

func parseApplianceFrontends(values []string) ([]*appliancev1.ControllerFrontend, error) {
	var frontends []*appliancev1.ControllerFrontend
	for _, value := range values {
		name, exposure, ok := strings.Cut(value, "=")
		if !ok || name == "" {
			return nil, errors.New("frontend requires NAME=loopback|outbound-only|lan-http")
		}
		modes := map[string]appliancev1.FrontendExposure{"loopback": appliancev1.FrontendExposure_FRONTEND_EXPOSURE_HOST_LOOPBACK, "outbound-only": appliancev1.FrontendExposure_FRONTEND_EXPOSURE_OUTBOUND_ONLY, "lan-http": appliancev1.FrontendExposure_FRONTEND_EXPOSURE_LAN_HTTP}
		mode, ok := modes[exposure]
		if !ok {
			return nil, fmt.Errorf("unknown frontend exposure %q", exposure)
		}
		frontends = append(frontends, &appliancev1.ControllerFrontend{Name: name, Transport: appliancev1.FrontendTransport_FRONTEND_TRANSPORT_HTTP, Exposure: mode, Bounds: &appliancev1.StreamBounds{MaxFrameBytes: 1 << 20, MaxTotalBytes: 1 << 30, DeadlineSeconds: 3600, IdleSeconds: 300, MaxConcurrentStreams: 16}})
	}
	return frontends, nil
}
