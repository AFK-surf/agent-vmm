//go:build darwin

package main

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type cliLifecycleServer struct {
	appliancev1.UnimplementedApplianceAdminServiceServer
	requests       chan string
	removeMu       sync.Mutex
	removeRequest  *appliancev1.ExecuteRemoveRequest
	loseRemoveACK  bool
	removeFailure  error
	removeResponse *appliancev1.ApplianceOperationResponse
	watch          func(*appliancev1.WatchApplianceOperationRequest, grpc.ServerStreamingServer[appliancev1.ApplianceOperationResponse]) error
}

func (s *cliLifecycleServer) List(context.Context, *appliancev1.ListAppliancesRequest) (*appliancev1.ListAppliancesResponse, error) {
	return &appliancev1.ListAppliancesResponse{Appliances: []*appliancev1.ApplianceStatus{{InstallationId: "installation", ProductId: "product", Revision: 7}}}, nil
}
func (s *cliLifecycleServer) GetStatus(context.Context, *appliancev1.GetApplianceStatusRequest) (*appliancev1.ApplianceStatusResponse, error) {
	return &appliancev1.ApplianceStatusResponse{Status: &appliancev1.ApplianceStatus{InstallationId: "installation", Revision: 7}}, nil
}
func (s *cliLifecycleServer) PlanRemove(_ context.Context, r *appliancev1.PlanRemoveRequest) (*appliancev1.AppliancePlanResponse, error) {
	if r.GetInstallationId() != "installation" || r.GetExpectedRevision() != 7 || r.GetRequestId() != "remove-request" {
		return nil, status.Error(codes.InvalidArgument, "incorrect removal plan")
	}
	s.requests <- "remove-plan"
	return &appliancev1.AppliancePlanResponse{Plan: &appliancev1.AppliancePlan{InstallationId: "installation", Revision: 7, Impacts: []*appliancev1.ApplianceImpact{{ResourceType: "data_volume", ResourceId: "data-to-delete", Effect: "delete", Irreversible: true}, {ResourceType: "data_volume", ResourceId: "second-data-to-delete", Effect: "delete", Irreversible: true}}}}, nil
}
func (s *cliLifecycleServer) ExecuteRemove(_ context.Context, r *appliancev1.ExecuteRemoveRequest) (*appliancev1.ApplianceOperationResponse, error) {
	s.removeMu.Lock()
	defer s.removeMu.Unlock()
	response := s.removeResponse
	if response == nil {
		response = cliOperation()
	}
	if s.removeFailure != nil {
		return nil, s.removeFailure
	}
	if s.removeRequest != nil {
		if !proto.Equal(s.removeRequest, r) {
			return nil, status.Error(codes.AlreadyExists, "original remove fencing changed")
		}
		s.requests <- "remove-execute"
		return response, nil
	}
	if r.GetInstallationId() != "installation" || r.GetExpectedRevision() != 7 || r.GetRequestId() != "remove-request" || r.GetDeadlineUnixMillis() <= time.Now().UnixMilli() || len(r.GetConfirmedResourceIds()) != 2 || r.GetConfirmedResourceIds()[0] != "data-to-delete" || r.GetConfirmedResourceIds()[1] != "second-data-to-delete" {
		return nil, status.Error(codes.InvalidArgument, "incorrect removal confirmation")
	}
	s.removeRequest = proto.Clone(r).(*appliancev1.ExecuteRemoveRequest)
	s.requests <- "remove-execute"
	if s.loseRemoveACK {
		return nil, status.Error(codes.Unavailable, "accepted remove response lost")
	}
	return response, nil
}
func (s *cliLifecycleServer) PlanActivation(_ context.Context, r *appliancev1.PlanActivationRequest) (*appliancev1.AppliancePlanResponse, error) {
	s.requests <- "plan"
	return &appliancev1.AppliancePlanResponse{}, nil
}
func (s *cliLifecycleServer) GetContentStatus(context.Context, *appliancev1.GetContentStatusRequest) (*appliancev1.GetContentStatusResponse, error) {
	return &appliancev1.GetContentStatusResponse{}, nil
}
func (s *cliLifecycleServer) StageArtifact(context.Context, *appliancev1.StageArtifactRequest) (*appliancev1.ApplianceOperationResponse, error) {
	return &appliancev1.ApplianceOperationResponse{ArtifactId: "artifact"}, nil
}
func (s *cliLifecycleServer) PlanInstall(_ context.Context, r *appliancev1.PlanInstallRequest) (*appliancev1.AppliancePlanResponse, error) {
	if r.GetApprovedGrant().GetCapacity().GetPerEnvironmentLimits().GetDiskBytes() != 4<<30 || r.GetApprovedGrant().GetSystemLimits().GetPids() != 256 {
		return nil, status.Error(codes.InvalidArgument, "incorrect resource flags")
	}
	return nil, status.Error(codes.ResourceExhausted, "approved resources exceed VM capacity")
}
func cliOperation() *appliancev1.ApplianceOperationResponse {
	return &appliancev1.ApplianceOperationResponse{Operation: &appliancev1.ApplianceOperation{OperationId: "operation", InstallationId: "installation", State: appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED, Phase: appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL}}
}
func (s *cliLifecycleServer) Stop(_ context.Context, r *appliancev1.StopApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	s.requests <- "stop"
	if r.GetExpectedRevision() != 7 || r.GetRequestId() == "" || r.GetInstallationId() != "installation" {
		panic("stop lost its CAS identity")
	}
	return cliOperation(), nil
}
func (s *cliLifecycleServer) Revoke(_ context.Context, r *appliancev1.RevokeApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	s.requests <- "revoke"
	if r.GetExpectedRevision() != 7 || r.GetRequestId() == "" || r.GetInstallationId() != "installation" || r.GetDeadlineUnixMillis() <= time.Now().UnixMilli() {
		panic("revoke lost its CAS identity or deadline")
	}
	return cliOperation(), nil
}
func (s *cliLifecycleServer) Restart(_ context.Context, r *appliancev1.RestartApplianceRequest) (*appliancev1.ApplianceOperationResponse, error) {
	s.requests <- "restart"
	if r.GetExpectedRevision() != 7 || r.GetRequestId() == "" {
		panic("restart lost its CAS identity")
	}
	return cliOperation(), nil
}
func (s *cliLifecycleServer) Activate(_ context.Context, r *appliancev1.ActivateRequest) (*appliancev1.ApplianceOperationResponse, error) {
	s.requests <- "activate"
	if r.GetExpectedRevision() != 7 || r.GetRequestId() == "" {
		panic("activate lost its CAS identity")
	}
	return cliOperation(), nil
}
func (s *cliLifecycleServer) WatchOperation(request *appliancev1.WatchApplianceOperationRequest, stream grpc.ServerStreamingServer[appliancev1.ApplianceOperationResponse]) error {
	if s.watch != nil {
		return s.watch(request, stream)
	}
	return stream.Send(cliOperation())
}

func TestManagedOnlyBinaryExposesApplianceLifecycle(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "agent-vmm")
	build := exec.Command("go", "build", "-tags=managedonly", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("managed CLI build: %v\n%s", err, output)
	}
	t.Setenv("HOME", t.TempDir())
	for _, command := range []string{"install", "update", "status", "recover", "stop", "start", "activate", "restart", "revoke", "remove"} {
		output, err := exec.Command(binary, "appliance", command, "--help").CombinedOutput()
		if err != nil || !strings.Contains(string(output), "Usage of appliance "+command) {
			t.Fatalf("%s help: %v\n%s", command, err, output)
		}
		if command == "install" || command == "update" {
			for _, name := range []string{"system-pids", "system-disk-gib", "environment-pids", "environment-disk-gib"} {
				if !strings.Contains(string(output), "-"+name) {
					t.Fatalf("missing help flag %s", name)
				}
			}
		}
	}
	if output, err := exec.Command(binary, "appliance", "update", "--unknown-flag").CombinedOutput(); err == nil || !strings.Contains(string(output), "flag provided but not defined") {
		t.Fatalf("invalid lifecycle flag: %v\n%s", err, output)
	}
	socketDir, err := os.MkdirTemp("/tmp", "vmm-cli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDir)
	socket := filepath.Join(socketDir, "operator.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	defer server.Stop()
	service := &cliLifecycleServer{requests: make(chan string, 8), loseRemoveACK: true}
	appliancev1.RegisterApplianceAdminServiceServer(server, service)
	go server.Serve(listener)
	packageDir := t.TempDir()
	limits := &hostv1.ResourceLimits{Pids: proto.Uint32(1024), DiskBytes: proto.Uint64(16 << 30)}
	manifestBytes, err := proto.Marshal(&appliancev1.ControllerApplianceManifest{ProductId: "product", SystemLimits: limits, RequestedCapacity: &appliancev1.CapacityGrant{PerEnvironmentLimits: limits}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "manifest.pb"), manifestBytes, 0600); err != nil {
		t.Fatal(err)
	}
	install := exec.Command(binary, "appliance", "install", "--operator-socket", socket, "--package", packageDir, "--bootstrap-stdin", "--environment-disk-gib=4", "--system-pids=256")
	install.Stdin = strings.NewReader("test-seed")
	output, installErr := install.CombinedOutput()
	if installErr == nil || !strings.Contains(string(output), "install plan rejected") || strings.Contains(string(output), "panic:") || strings.Contains(string(output), "test-seed") {
		t.Fatalf("plan error: %v\n%s", installErr, output)
	}
	removeArgs := []string{"appliance", "remove", "--operator-socket", socket, "--installation", "installation", "--request-id", "remove-request", "--timeout", "5s"}
	output, err = exec.Command(binary, removeArgs...).CombinedOutput()
	if err != nil || !strings.Contains(string(output), "expected_revision=7") || !strings.Contains(string(output), "data-to-delete") {
		t.Fatalf("removal plan: %v\n%s", err, output)
	}
	if got := <-service.requests; got != "remove-plan" {
		t.Fatalf("planning performed %s", got)
	}
	missing := append(append([]string{}, removeArgs...), "--execute", "--expected-revision", "7")
	if output, err := exec.Command(binary, missing...).CombinedOutput(); err == nil || !strings.Contains(string(output), "explicit --confirm-resource") {
		t.Fatalf("unconfirmed removal: %v\n%s", err, output)
	}
	select {
	case got := <-service.requests:
		t.Fatalf("unconfirmed removal called %s", got)
	default:
	}
	confirmed := append(missing, "--confirm-resource", "data-to-delete", "--confirm-resource", "second-data-to-delete")
	if output, err := exec.Command(binary, confirmed...).CombinedOutput(); err == nil || !strings.Contains(string(output), "accepted remove response lost") {
		t.Fatalf("lost removal ACK: %v\n%s", err, output)
	}
	if got := <-service.requests; got != "remove-execute" {
		t.Fatalf("initial execution performed %s", got)
	}
	reordered := append(append([]string{}, missing...), "--confirm-resource", "second-data-to-delete", "--confirm-resource", "data-to-delete", "--confirm-resource", "data-to-delete")
	for _, retryArguments := range [][]string{reordered, confirmed} {
		if output, err := exec.Command(binary, retryArguments...).CombinedOutput(); err != nil {
			t.Fatalf("confirmed removal/replay: %v\n%s", err, output)
		}
		if got := <-service.requests; got != "remove-execute" {
			t.Fatalf("execution performed %s", got)
		}
	}
	for _, changed := range []struct{ flag, value string }{
		{"--expected-revision", "8"}, {"--confirm-resource", "other-data"}, {"--timeout", "6s"},
	} {
		conflict := append([]string{}, confirmed...)
		for index := 0; index+1 < len(conflict); index++ {
			if conflict[index] == changed.flag {
				conflict[index+1] = changed.value
				break
			}
		}
		if output, err := exec.Command(binary, conflict...).CombinedOutput(); err == nil || !strings.Contains(string(output), "different confirmed command") {
			t.Fatalf("changed removal %s accepted: %v\n%s", changed.flag, err, output)
		}
		select {
		case got := <-service.requests:
			t.Fatalf("conflicting command performed %s", got)
		default:
		}
	}
	for _, command := range []string{"stop", "start", "restart", "revoke"} {
		output, err := exec.Command(binary, "appliance", command, "--operator-socket", socket, "--product", "product", "--timeout", "5s").CombinedOutput()
		if err != nil || !strings.Contains(string(output), "operation=operation") {
			t.Fatalf("%s dispatch: %v\n%s", command, err, output)
		}
		expected := []string{command}
		if command == "start" {
			expected = []string{"plan", "activate"}
		}
		for _, want := range expected {
			if got := <-service.requests; got != want {
				t.Fatalf("dispatch=%s want=%s", got, want)
			}
		}
	}
}

func TestApplianceRemoveExpiredFrozenIntentUsesAuthoritativeAdmission(t *testing.T) {
	for _, mode := range []string{"accepted", "not-accepted", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			directory, err := os.MkdirTemp("/tmp", "vmm-expired-cli-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(directory)
			socket := filepath.Join(directory, "operator.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			request := &appliancev1.ExecuteRemoveRequest{InstallationId: "installation", RequestId: "remove-request", ExpectedRevision: 7, DeadlineUnixMillis: time.Now().Add(-time.Minute).UnixMilli(), ConfirmedResourceIds: []string{"data-to-delete", "second-data-to-delete"}}
			if _, err := freezeApplianceRemove(socket, request, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			service := &cliLifecycleServer{requests: make(chan string, 8), removeRequest: proto.Clone(request).(*appliancev1.ExecuteRemoveRequest)}
			if mode == "not-accepted" {
				failure, err := status.New(codes.DeadlineExceeded, "intent expired").WithDetails(&errdetails.ErrorInfo{Reason: "APPLIANCE_OPERATION_NOT_ADMITTED", Domain: "agentvmm.afk.surf", Metadata: map[string]string{"admission": "not_accepted"}})
				if err != nil {
					t.Fatal(err)
				}
				service.removeFailure = failure.Err()
			} else if mode == "unknown" {
				service.removeFailure = status.Error(codes.Unavailable, "transport unknown")
			}
			server := grpc.NewServer()
			defer server.Stop()
			appliancev1.RegisterApplianceAdminServiceServer(server, service)
			go server.Serve(listener)
			err = runApplianceRemove(socket, "installation", "remove-request", 7, true, request.ConfirmedResourceIds, 5*time.Second)
			if mode == "accepted" && err != nil {
				t.Fatal("accepted expired intent was not observed", err)
			}
			if mode == "not-accepted" && (err == nil || !strings.Contains(err.Error(), "appliance remove --installation installation") || !strings.Contains(err.Error(), "new --request-id")) {
				t.Fatal("non-admission omitted fresh plan entry", err)
			}
			if mode == "unknown" && (status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), "new plan")) {
				t.Fatal("unknown transport was treated as non-admission", err)
			}
			frozen, err := freezeApplianceRemove(socket, request, 5*time.Second)
			if err != nil || frozen.DeadlineUnixMillis != request.DeadlineUnixMillis {
				t.Fatal("retry extended frozen intent", frozen, err)
			}
		})
	}
}

func TestApplianceRemoveWatchDeadlineRetainsOriginalCommand(t *testing.T) {
	for _, outcome := range []string{"timeout", "succeeded", "failed", "unavailable", "server-deadline"} {
		t.Run(outcome, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			directory, err := os.MkdirTemp("/tmp", "vmm-watch-'")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(directory)
			socket := filepath.Join(directory, "operator $.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			operation := &appliancev1.ApplianceOperationResponse{Operation: &appliancev1.ApplianceOperation{InstallationId: "installation", OperationId: "original-operation", RequestId: "remove-request", State: appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING, Phase: appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY}}
			service := &cliLifecycleServer{requests: make(chan string, 1), removeResponse: operation}
			service.watch = func(request *appliancev1.WatchApplianceOperationRequest, stream grpc.ServerStreamingServer[appliancev1.ApplianceOperationResponse]) error {
				if request.GetInstallationId() != "installation" || request.GetOperationId() != "original-operation" {
					return status.Error(codes.InvalidArgument, "watch identity changed")
				}
				switch outcome {
				case "succeeded":
					return stream.Send(cliOperation())
				case "failed":
					failed := proto.Clone(operation).(*appliancev1.ApplianceOperationResponse)
					failed.Operation.State = appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_FAILED
					failed.Operation.Error = &appliancev1.ApplianceOperationError{Reason: "REMOVE_FAILED"}
					return stream.Send(failed)
				case "unavailable":
					return status.Error(codes.Unavailable, "watch unavailable")
				case "server-deadline":
					return status.Error(codes.DeadlineExceeded, "server deadline")
				default:
					if err := stream.Send(operation); err != nil {
						return err
					}
					<-stream.Context().Done()
					return stream.Context().Err()
				}
			}
			server := grpc.NewServer()
			defer server.Stop()
			appliancev1.RegisterApplianceAdminServiceServer(server, service)
			go server.Serve(listener)
			err = runApplianceRemove(socket, "installation", "remove-request", 7, true, []string{"second-data-to-delete", "data-to-delete"}, time.Second)
			if outcome == "succeeded" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("missing watch error")
			}
			if outcome != "timeout" && outcome != "server-deadline" {
				if strings.Contains(err.Error(), "Continue") || strings.Contains(err.Error(), "request_id=") {
					t.Fatal("non-deadline received continuation guidance", err)
				}
				if outcome == "failed" && !strings.Contains(err.Error(), "REMOVE_FAILED") || outcome == "unavailable" && status.Code(err) != codes.Unavailable {
					t.Fatal("watch error changed", err)
				}
				return
			}
			if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "installation=installation operation=original-operation request_id=remove-request") {
				t.Fatal("watch deadline lost operation identity or original error", err)
			}
			lines := strings.Split(err.Error(), "\n")
			command := lines[len(lines)-2]
			parsed, parseErr := exec.Command("/bin/sh", "-c", "set -- "+command+"; printf '%s\\000' \"$@\"").Output()
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			want := []string{os.Args[0], "appliance", "remove", "--operator-socket", socket, "--installation", "installation", "--request-id", "remove-request", "--execute", "--expected-revision", "7", "--timeout", "1s", "--confirm-resource", "data-to-delete", "--confirm-resource", "second-data-to-delete", ""}
			if string(parsed) != strings.Join(want, "\x00") {
				t.Fatalf("continuation command changed arguments: %q", parsed)
			}
		})
	}
}
