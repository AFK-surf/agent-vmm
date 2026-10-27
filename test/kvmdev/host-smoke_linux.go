//go:build linux && hostsmoke

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type hostSmokeOptions struct {
	operator                string
	workerSocket            string
	workerBinary            string
	directory               string
	image                   string
	trustedURL              string
	invalidURL              string
	runID                   string
	timeout                 time.Duration
	requireRegistryDownload bool
}

func main() {
	var opts hostSmokeOptions
	flag.StringVar(&opts.operator, "operator-socket", "", "running Host operator socket")
	flag.StringVar(&opts.workerSocket, "data-worker-socket", "", "Host data worker control socket")
	flag.StringVar(&opts.workerBinary, "data-worker", "", "Linux data worker executable for the disposable peer")
	flag.StringVar(&opts.directory, "directory", "", "private experiment directory for disposable peer files")
	flag.StringVar(&opts.image, "image", "quay.io/curl/curl:latest", "public arm64 image with curl and /bin/sh")
	flag.StringVar(&opts.trustedURL, "https-url", "https://example.com", "public HTTPS endpoint with a trusted certificate")
	flag.StringVar(&opts.invalidURL, "invalid-cert-url", "https://self-signed.badssl.com/", "public HTTPS endpoint with an untrusted certificate")
	flag.StringVar(&opts.runID, "run-id", fmt.Sprintf("linux-host-smoke-%d", time.Now().Unix()), "request and environment identity")
	flag.BoolVar(&opts.requireRegistryDownload, "require-registry-download", true, "require fresh registry download progress instead of cached image availability")
	flag.DurationVar(&opts.timeout, "timeout", 8*time.Minute, "complete probe deadline")
	flag.Parse()
	for _, path := range []string{opts.operator, opts.workerSocket, opts.workerBinary, opts.directory} {
		if !filepath.IsAbs(path) {
			fmt.Fprintln(os.Stderr, "operator, worker, executable, and directory paths must be absolute")
			os.Exit(2)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	if err := runHostSmoke(ctx, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runHostSmoke(ctx context.Context, opts hostSmokeOptions) error {
	if err := smokeDataWorker(ctx, opts); err != nil {
		return fmt.Errorf("data worker path: %w", err)
	}
	connection, err := smokeUnixClient(opts.operator)
	if err != nil {
		return err
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	vm, err := operator.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		return fmt.Errorf("Host VM status: %w", err)
	}
	fmt.Printf("Host initial VM state=%s boot=%s\n", vm.GetVm().GetState(), vm.GetVm().GetBootId())
	limits := &hostv1.ResourceLimits{Pids: proto.Uint32(128), DiskBytes: proto.Uint64(128 << 20)}
	prepared, err := operator.PrepareAgentIntegration(ctx, &hostv1.PrepareAgentIntegrationRequest{RequestId: opts.runID + "-prepare", AdapterId: "linux-host-smoke", DisplayName: opts.runID, Scope: hostv1.IntegrationScope_INTEGRATION_SCOPE_MANUAL, ConfigTarget: "smoke://" + opts.runID, EnvironmentId: proto.String(opts.runID), Limits: limits})
	if err != nil {
		return fmt.Errorf("prepare manual integration: %w", err)
	}
	committed, err := operator.CommitAgentIntegration(ctx, &hostv1.CommitAgentIntegrationRequest{RequestId: opts.runID + "-commit", IntegrationId: prepared.GetIntegration().GetId(), ExpectedRevision: prepared.GetIntegration().GetRevision(), ConfigFingerprint: opts.runID})
	if err != nil {
		return fmt.Errorf("commit manual integration: %w", err)
	}
	session, err := operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: opts.runID + "-session", IntegrationId: committed.GetIntegration().GetId()})
	if err != nil {
		return fmt.Errorf("create agent session/start VM: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = operator.RevokeAgentSession(cleanup, &hostv1.RevokeAgentSessionRequest{RequestId: opts.runID + "-revoke-session", SessionId: session.GetSessionId()})
	}()
	_, err = operator.UpdateEnvironmentPolicy(ctx, &hostv1.UpdateEnvironmentPolicyRequest{RequestId: opts.runID + "-public", EnvironmentId: opts.runID, Limits: limits, EgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, ExpectedRevision: session.GetEnvironment().GetPolicyRevision()})
	if err != nil {
		return fmt.Errorf("enable public policy: %w", err)
	}
	agentConnection, err := smokeUnixClient(session.GetSocketPath())
	if err != nil {
		return err
	}
	defer agentConnection.Close()
	agent := hostv1.NewAgentRuntimeServiceClient(agentConnection)
	fmt.Printf("PASS Host operator/manual integration/session namespace=%s public Internet policy\n", opts.runID)
	pull, err := agent.PullImage(ctx, &hostv1.PullImageRequest{RequestId: opts.runID + "-pull", Reference: opts.image})
	if err != nil {
		return fmt.Errorf("public registry pull: %w", err)
	}
	digest := ""
	downloaded := false
	for {
		progress, err := pull.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("public registry pull: %w", err)
		}
		fmt.Printf("registry pull status=%s digest=%s bytes=%d\n", progress.GetStatus(), progress.GetDigest(), progress.GetLogicalBytes())
		if progress.GetStatus() == "downloading from registry" {
			downloaded = true
		}
		if progress.GetDigest() != "" {
			digest = progress.GetDigest()
		}
	}
	if digest == "" {
		return errors.New("public registry pull returned no digest")
	}
	if downloaded {
		fmt.Printf("PASS fresh public registry TLS download image=%s digest=%s\n", opts.image, digest)
	} else {
		fmt.Printf("PASS cached image availability image=%s digest=%s\n", opts.image, digest)
		if opts.requireRegistryDownload {
			return errors.New("cached image does not prove fresh registry TLS download; use --require-registry-download=false only for an availability rerun and retain the earlier cold-download evidence")
		}
	}
	script := `curl --fail --silent --show-error --connect-timeout 10 --max-time 30 "$1" -o /dev/null
 echo HOST_SMOKE_TRUSTED_HTTPS_OK
 set +e
 curl --silent --show-error --connect-timeout 10 --max-time 30 "$2" -o /dev/null
 certificate_exit=$?
 set -e
 test "$certificate_exit" -eq 60
 echo HOST_SMOKE_UNTRUSTED_CERT_REJECTED
 `
	created, err := agent.CreateContainer(ctx, &hostv1.CreateContainerRequest{RequestId: opts.runID + "-create", ContainerId: "https", Image: opts.image, Limits: &hostv1.ResourceLimits{Pids: proto.Uint32(64)}, ReadOnlyRoot: true, LogLimitBytes: 64 << 10, EntrypointOverride: &hostv1.StringList{Values: []string{"/bin/sh"}}, CommandOverride: &hostv1.StringList{Values: []string{"-ec", script, "host-smoke", opts.trustedURL, opts.invalidURL}}})
	if err != nil {
		return fmt.Errorf("create HTTPS workload: %w", err)
	}
	started, err := agent.StartContainer(ctx, &hostv1.StartContainerRequest{RequestId: opts.runID + "-start", ContainerId: "https", ExpectedGenerationId: created.GetContainer().GetGenerationId()})
	if err != nil {
		return fmt.Errorf("start HTTPS workload: %w", err)
	}
	fmt.Printf("gVisor workload instance=%s\n", started.GetContainer().GetInstanceId())
	waited, err := agent.WaitContainer(ctx, &hostv1.WaitContainerRequest{ContainerId: "https"})
	if err != nil {
		return fmt.Errorf("wait HTTPS workload: %w", err)
	}
	logs, err := agent.Logs(ctx, &hostv1.LogsRequest{ContainerId: "https", TailRecords: 100})
	if err != nil {
		return fmt.Errorf("HTTPS workload logs: %w", err)
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
		fmt.Printf("workload %s: %s", record.GetStream(), record.GetData())
		output.Write(record.GetData())
	}
	if waited.GetExitCode() != 0 {
		return fmt.Errorf("HTTPS workload exit=%d", waited.GetExitCode())
	}
	for _, marker := range []string{"HOST_SMOKE_TRUSTED_HTTPS_OK", "HOST_SMOKE_UNTRUSTED_CERT_REJECTED"} {
		if !strings.Contains(output.String(), marker) {
			return fmt.Errorf("missing workload evidence %s", marker)
		}
	}
	fmt.Println("PASS Host namespace gVisor trusted HTTPS and curl60 invalid-certificate rejection")
	return nil
}

func smokeUnixClient(socket string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///host-smoke", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
}

func smokeDataWorker(ctx context.Context, opts hostSmokeOptions) error {
	observed, err := dataworker.GetStatus(ctx, opts.workerSocket)
	if err != nil {
		return err
	}
	if observed.EndpointID == "" || observed.EndpointGeneration == 0 || !observed.Accepting {
		return errors.New("Host data worker is not ready")
	}
	directory, err := os.MkdirTemp(opts.directory, "host-smoke-peer-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	peer, err := dataworker.Start(ctx, opts.workerBinary, filepath.Join(directory, "peer.sock"), filepath.Join(directory, "unused-service.sock"), filepath.Join(directory, "unused-mesh.sock"))
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = peer.Close(cleanup)
	}()
	address, err := smokeLoopbackEndpoint(observed.EndpointAddr)
	if err != nil {
		return err
	}
	tunnel, err := grpc.NewClient("passthrough:///host-smoke-quic", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return dataworker.DialMeshControlTunnel(ctx, filepath.Join(directory, "peer.sock"), address)
	}))
	if err != nil {
		return err
	}
	defer tunnel.Close()
	probe, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err = servicev1.NewMeshControlServiceClient(tunnel).RequestPersonalRoute(probe, &servicev1.RequestPersonalRouteRequest{RequestId: opts.runID + "-mesh-probe", DestinationExportId: opts.runID + "-missing-export"})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("Host MeshControl through Rust QUIC peer returned %v; require NotFound", err)
	}
	if err := tunnel.Close(); err != nil {
		return err
	}
	accounting, stopAccounting := context.WithTimeout(ctx, 3*time.Second)
	defer stopAccounting()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var after dataworker.Status
	for {
		after, err = dataworker.GetStatus(accounting, opts.workerSocket)
		if err != nil {
			return err
		}
		if after.BytesIn > observed.BytesIn && after.BytesOut > observed.BytesOut {
			break
		}
		select {
		case <-accounting.Done():
			return fmt.Errorf("closed Rust tunnel accounting did not settle: before=%d/%d after=%d/%d active=%d: %w", observed.BytesIn, observed.BytesOut, after.BytesIn, after.BytesOut, after.ActiveStreams, accounting.Err())
		case <-ticker.C:
		}
	}
	fmt.Printf("PASS Host Rust data-worker ready endpoint=%s generation=%d and real QUIC MeshControl gRPC path bytes-in=%d bytes-out=%d\n", observed.EndpointID, observed.EndpointGeneration, after.BytesIn-observed.BytesIn, after.BytesOut-observed.BytesOut)
	return nil
}

func smokeLoopbackEndpoint(encoded json.RawMessage) (json.RawMessage, error) {
	var address struct {
		ID    string           `json:"id"`
		Addrs []map[string]any `json:"addrs"`
	}
	if err := json.Unmarshal(encoded, &address); err != nil {
		return nil, err
	}
	for _, candidate := range address.Addrs {
		if ip, ok := candidate["Ip"].(string); ok {
			_, port, err := net.SplitHostPort(ip)
			if err != nil {
				return nil, err
			}
			address.Addrs = append(address.Addrs, map[string]any{"Ip": net.JoinHostPort("127.0.0.1", port)})
			return json.Marshal(address)
		}
	}
	return nil, errors.New("Host data worker has no direct IP endpoint for the local peer")
}
