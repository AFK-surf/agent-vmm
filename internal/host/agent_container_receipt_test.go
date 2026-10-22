//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestContainerConfigurationFingerprintCoversCreationIntent(t *testing.T) {
	request := &hostv1.CreateContainerRequest{RequestId: "create", ContainerId: "web", Image: "image", Env: []string{"CONFIG=first"}, Limits: &hostv1.ResourceLimits{MemoryBytes: optional(uint64(256))}, Volumes: []*hostv1.VolumeMount{{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "data"}, Destination: "/data"}}, LogLimitBytes: 4096}
	original := proto.Clone(request)
	fingerprint := containerConfigurationFingerprint(request)
	replay := proto.Clone(request).(*hostv1.CreateContainerRequest)
	replay.RequestId = "different-create-request"
	if len(fingerprint) != 64 || containerConfigurationFingerprint(replay) != fingerprint || !proto.Equal(request, original) {
		t.Fatal("request identity affected configuration or fingerprint mutated the request")
	}
	for name, change := range map[string]func(*hostv1.CreateContainerRequest){
		"environment value": func(r *hostv1.CreateContainerRequest) { r.Env[0] = "CONFIG=second" },
		"resource":          func(r *hostv1.CreateContainerRequest) { r.Limits.MemoryBytes = optional(uint64(512)) },
		"mount source": func(r *hostv1.CreateContainerRequest) {
			r.Volumes[0].Source = &hostv1.VolumeMount_HostShare{HostShare: "data"}
		},
		"volume identity": func(r *hostv1.CreateContainerRequest) {
			r.Volumes[0].Source = &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "replacement"}
		},
		"volume destination": func(r *hostv1.CreateContainerRequest) { r.Volumes[0].Destination = "/other" },
		"volume permission":  func(r *hostv1.CreateContainerRequest) { r.Volumes[0].ReadOnly = true },
		"entrypoint": func(r *hostv1.CreateContainerRequest) {
			r.EntrypointOverride = &hostv1.StringList{Values: []string{"daemon"}}
		},
		"command": func(r *hostv1.CreateContainerRequest) {
			r.CommandOverride = &hostv1.StringList{Values: []string{"serve"}}
		},
		"user":          func(r *hostv1.CreateContainerRequest) { r.User = optional("1000") },
		"workdir":       func(r *hostv1.CreateContainerRequest) { r.Workdir = optional("/data") },
		"readonly root": func(r *hostv1.CreateContainerRequest) { r.ReadOnlyRoot = true },
		"log limit":     func(r *hostv1.CreateContainerRequest) { r.LogLimitBytes++ },
		"image":         func(r *hostv1.CreateContainerRequest) { r.Image = "other" },
		"container":     func(r *hostv1.CreateContainerRequest) { r.ContainerId = "worker" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(request).(*hostv1.CreateContainerRequest)
			change(changed)
			if containerConfigurationFingerprint(changed) == fingerprint {
				t.Fatal("changed configuration retained fingerprint")
			}
		})
	}
}

func TestContainerCreateAdmissionFailureReceiptSurvivesRestart(t *testing.T) {
	for _, stage := range []string{"import_slot", "import_admission", "rootfs_snapshot"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			server := &agentServer{controller: &Controller{store: database}}
			identity := auth.Identity{EnvironmentID: "environment", NamespaceID: "namespace", SessionID: "original-session"}
			request := &hostv1.CreateContainerRequest{RequestId: "create", ContainerId: "runner"}
			failure, err := status.New(codes.ResourceExhausted, "no physical headroom").WithDetails(&errdetails.ErrorInfo{Reason: "RESOURCE_CAPACITY_EXHAUSTED", Domain: "agent-vmm", Metadata: map[string]string{"stage": stage}})
			if err != nil {
				t.Fatal(err)
			}
			if stage != "rootfs_snapshot" {
				failure, err = failure.WithDetails(&errdetails.ErrorInfo{Reason: "CONTAINER_CREATE_NOT_ADMITTED", Domain: "agent-vmm", Metadata: map[string]string{"admission": "not_accepted"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			execute := func() (*hostv1.ContainerResponse, error) { calls++; return nil, failure.Err() }
			_, first := server.containerMutationReceipt(context.Background(), identity, "runtime-create-container", request.RequestId, request, request, "runner", execute, admitOperation(server))
			if status.Code(first) != codes.ResourceExhausted {
				t.Fatal(first)
			}
			database.Close()
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			server.controller.store = database
			key := sha256.Sum256([]byte("runtime-create-container\x00" + request.RequestId))
			operation, err := database.GetOperation(context.Background(), "runtime-"+hex.EncodeToString(key[:]))
			if err != nil {
				t.Fatal(err)
			}
			if stage == "rootfs_snapshot" {
				if operation.State == int32(hostv1.OperationState_OPERATION_STATE_FAILED) || len(operation.Receipt) != 0 {
					t.Fatal("uncertain materialization failure became terminal")
				}
				return
			}
			if operation.State != int32(hostv1.OperationState_OPERATION_STATE_FAILED) {
				t.Fatal("admission rejection lost terminal receipt")
			}
			_, replay := server.containerMutationReceipt(context.Background(), identity, "runtime-create-container", request.RequestId, request, request, "runner", execute, admitOperation(server))
			if !proto.Equal(status.Convert(first).Proto(), status.Convert(replay).Proto()) || calls != 1 {
				t.Fatalf("replay=%v calls=%d", replay, calls)
			}
		})
	}
}

func TestContainerMutationReceiptSurvivesDeletionAndHostRestart(t *testing.T) {
	for _, kind := range []string{"runtime-create-container", "runtime-start-container"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			server := &agentServer{controller: &Controller{store: database}}
			identity := auth.Identity{EnvironmentID: "environment", NamespaceID: "namespace", SessionID: "original-session"}
			if kind == "runtime-start-container" {
				if _, err := database.CreateEnvironment(t.Context(), store.Environment{ID: identity.EnvironmentID, NamespaceID: identity.NamespaceID, DisplayName: "Service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
					t.Fatal(err)
				}
				if _, err := database.CreateAgentIntegration(t.Context(), store.AgentIntegration{ID: "service", EnvironmentID: identity.EnvironmentID, AdapterID: "agentd-service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare-service"}); err != nil {
					t.Fatal(err)
				}
			}
			request := &hostv1.ContainerMutationRequest{RequestId: "same-request", ContainerId: "runner", ExpectedGenerationId: "generation-1"}
			guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.RequestId, ActorNamespaceId: identity.NamespaceID, ContainerId: "runner", ExpectedGenerationId: "generation-1"}
			calls := 0
			execute := func() (*hostv1.ContainerResponse, error) {
				calls++
				if kind == "runtime-start-container" {
					if err := database.SetManagedContainerRunning(t.Context(), identity.EnvironmentID, "runner", "generation-1", true); err != nil {
						return nil, err
					}
				}
				return &hostv1.ContainerResponse{Container: &hostv1.Container{Id: "runner", GenerationId: "generation-1", InstanceId: "instance-1", StartRequestInstanceId: "instance-1", State: hostv1.ContainerState_CONTAINER_STATE_RUNNING, Argv: []string{"sensitive-input"}, ConfigurationMatches: optional(true)}}, nil
			}
			first, err := server.containerMutationReceipt(context.Background(), identity, kind, request.RequestId, request, guestRequest, "runner", execute, admitOperation(server))
			if err != nil {
				t.Fatal(err)
			}
			if len(first.GetContainer().GetArgv()) != 0 {
				t.Fatal("receipt retained process arguments")
			}
			if first.GetContainer().ConfigurationMatches != nil {
				t.Fatal("receipt retained a per-query configuration comparison")
			}
			if first.GetContainer().GetStartRequestInstanceId() != "instance-1" {
				t.Fatal("receipt lost the successful Start instance")
			}
			if kind == "runtime-start-container" {
				if err := database.SetManagedContainerRunning(t.Context(), identity.EnvironmentID, "runner", "generation-1", false); err != nil {
					t.Fatal(err)
				}
			}
			database.Close()
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			server.controller.store = database
			identity.SessionID = "replacement-session"
			// The physical container is absent; a successful receipt does not inspect or recreate it.
			replay, err := server.containerMutationReceipt(context.Background(), identity, kind, request.RequestId, request, guestRequest, "runner", execute, admitOperation(server))
			if err != nil || !proto.Equal(first, replay) || calls != 1 {
				t.Fatalf("replay=%v calls=%d err=%v", replay, calls, err)
			}
			if kind == "runtime-start-container" {
				intent, err := database.GetManagedContainer(t.Context(), identity.EnvironmentID, "runner")
				if err != nil || intent.DesiredRunning {
					t.Fatal("old Start receipt revived stopped intent", intent, err)
				}
			}
			for _, changed := range []auth.Identity{{EnvironmentID: "other", NamespaceID: "other"}, {EnvironmentID: identity.EnvironmentID, NamespaceID: "other"}} {
				_, err := server.containerMutationReceipt(context.Background(), changed, kind, request.RequestId, request, guestRequest, "runner", execute, admitOperation(server))
				if status.Code(err) != codes.AlreadyExists {
					t.Fatalf("cross-owner receipt=%v", err)
				}
			}
			changed := proto.Clone(request).(*hostv1.ContainerMutationRequest)
			changed.ExpectedGenerationId = "replacement"
			_, err = server.containerMutationReceipt(context.Background(), identity, kind, request.RequestId, changed, guestRequest, "runner", execute, admitOperation(server))
			if status.Code(err) != codes.AlreadyExists || calls != 1 {
				t.Fatalf("changed payload=%v calls=%d", err, calls)
			}
		})
	}
}

func TestContainerMutationUnknownOnlyObservesMatchingGuestReceipt(t *testing.T) {
	for _, kind := range []string{"runtime-create-container", "runtime-start-container"} {
		t.Run(kind, func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			server := &agentServer{controller: &Controller{store: database}}
			identity := auth.Identity{EnvironmentID: "environment", NamespaceID: "namespace"}
			request := &hostv1.ContainerMutationRequest{RequestId: "lost-response", ContainerId: "runner", ExpectedGenerationId: "generation"}
			guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.RequestId, ActorNamespaceId: identity.NamespaceID, ContainerId: "runner", ExpectedGenerationId: "generation"}
			guest := &workloadGuest{states: map[string]*vmmdv1.Container{}, notFound: map[string]bool{identity.NamespaceID: true}}
			client := workloadGuestClient(t, guest)
			calls := 0
			execute := func() (*hostv1.ContainerResponse, error) {
				calls++
				return nil, status.Error(codes.Unavailable, "lost response")
			}
			if _, err := server.containerMutationReceipt(context.Background(), identity, kind, request.RequestId, request, guestRequest, "runner", execute, admitOperation(server)); status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}
			key := sha256.Sum256([]byte(kind + "\x00" + request.RequestId))
			accepted, err := database.GetOperation(context.Background(), "runtime-"+hex.EncodeToString(key[:]))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := server.resolveContainerMutation(context.Background(), client, identity, kind, guestRequest, "runner", accepted); status.Code(err) != codes.FailedPrecondition || calls != 1 {
				t.Fatalf("absent retry=%v calls=%d", err, calls)
			}
			guest.notFound[identity.NamespaceID] = false
			guest.states[identity.NamespaceID] = &vmmdv1.Container{Id: "runner", GenerationId: "replacement", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
			if _, err := server.resolveContainerMutation(context.Background(), client, identity, kind, guestRequest, "runner", accepted); status.Code(err) != codes.FailedPrecondition || calls != 1 {
				t.Fatalf("replacement retry=%v calls=%d", err, calls)
			}
			current := &vmmdv1.Container{Id: "runner", GenerationId: "generation", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED, ConfigurationFingerprint: optional("configuration")}
			hash := hex.EncodeToString(runtimeRequestHash(guestRequest))
			if kind == "runtime-create-container" {
				current.CreateRequestSha256 = hash
			} else {
				current.StartRequestSha256 = hash
				current.StartRequestInstanceId = "original-instance"
				current.StartedAtUnixNano = 1
			}
			guest.states[identity.NamespaceID] = current
			observed, err := server.resolveContainerMutation(context.Background(), client, identity, kind, guestRequest, "runner", accepted)
			if err != nil || calls != 1 || observed.GetContainer().GetGenerationId() != "generation" {
				t.Fatalf("matching receipt=%v calls=%d err=%v", observed, calls, err)
			}
			if observed.GetContainer().ConfigurationMatches != nil {
				t.Fatal("lost ACK receipt invented a configuration comparison")
			}
			if kind == "runtime-start-container" && (observed.GetContainer().GetInstanceId() != "original-instance" || observed.GetContainer().GetStartRequestInstanceId() != "original-instance") {
				t.Fatal("lost original start instance")
			}
		})
	}
}

func TestContainerStartPreflightCanRetrySameIdentityWithoutManagedIntent(t *testing.T) {
	for _, failure := range []string{"inspect", "generation", "share", "admission", "store"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { database.Close() }()
			if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "environment", NamespaceID: "namespace", OwnerKind: store.OwnerLocal}); err != nil {
				t.Fatal(err)
			}
			if _, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "integration", EnvironmentID: "environment", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"}); err != nil {
				t.Fatal(err)
			}
			controller := &Controller{store: database, workloadRunsRecovered: true}
			server := &agentServer{controller: controller}
			identity := auth.Identity{EnvironmentID: "environment", NamespaceID: "namespace"}
			request := &hostv1.ContainerMutationRequest{RequestId: "start", ContainerId: "runner", ExpectedGenerationId: "generation"}
			guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.RequestId, ActorNamespaceId: "namespace", ContainerId: "runner", ExpectedGenerationId: "generation"}
			container := &vmmdv1.Container{Id: "runner", GenerationId: "generation", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
			guest := &workloadGuest{states: map[string]*vmmdv1.Container{"namespace": container}, notFound: map[string]bool{}}
			client := workloadGuestClient(t, guest)
			sqlClient, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer sqlClient.Close()
			switch failure {
			case "inspect":
				guest.notFound["namespace"] = true
			case "generation":
				container.GenerationId = "changed"
			case "share":
				container.Volumes = []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: "missing"}, Destination: "/share"}}
			case "admission":
				controller.workloadRunsRecovered = false
			case "store":
				if _, err := sqlClient.Exec(`CREATE TRIGGER fail_start_intent BEFORE INSERT ON managed_containers BEGIN SELECT RAISE(ABORT,'intent save failed'); END`); err != nil {
					t.Fatal(err)
				}
			}
			call := func() (*hostv1.ContainerResponse, error) {
				unlock := controller.lockEnvironment(identity.EnvironmentID)
				defer unlock()
				var run *workloadRun
				var observed *vmmdv1.Container
				return server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.RequestId, request, guestRequest, "runner", func() (*hostv1.ContainerResponse, error) {
					response, err := controller.sendWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest, run, observed, time.Now().Add(time.Minute))
					if err != nil {
						return nil, err
					}
					return &hostv1.ContainerResponse{Container: containerFromGuest(response.Container)}, nil
				}, func(operation store.Operation) (store.Operation, bool, error) {
					run, observed, err = controller.prepareWorkloadStartLocked(ctx, client, identity.EnvironmentID, guestRequest)
					if err != nil {
						return store.Operation{}, false, err
					}
					operation, inserted, err := database.PutWorkloadStartOperation(ctx, operation, "runner", observed.GenerationId)
					if err != nil || !inserted {
						controller.releaseUnsentWorkloadStart(run)
					}
					return operation, inserted, err
				})
			}
			if _, err := call(); err == nil {
				t.Fatal("preflight unexpectedly succeeded")
			}
			key := sha256.Sum256([]byte("runtime-start-container\x00" + request.RequestId))
			if _, err := database.GetOperation(ctx, "runtime-"+hex.EncodeToString(key[:])); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("preflight left accepted receipt", err)
			}
			if _, err := database.GetManagedContainer(ctx, "environment", "runner"); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("preflight left background start intent", err)
			}
			if len(controller.workloadRuns) != 0 || guest.maxStarting != 0 {
				t.Fatal("preflight retained in-flight state or sent Guest start")
			}
			guest.notFound["namespace"] = false
			container.GenerationId, container.Volumes = "generation", nil
			controller.workloadRunsRecovered = true
			if _, err := sqlClient.Exec(`DROP TRIGGER IF EXISTS fail_start_intent`); err != nil {
				t.Fatal(err)
			}
			first, err := call()
			if err != nil {
				t.Fatal("same identity could not retry preflight", err)
			}
			if err := database.SetManagedContainerRunning(ctx, "environment", "runner", "generation", false); err != nil {
				t.Fatal(err)
			}
			database.Close()
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			controller.store = database
			replay, err := server.containerMutationReceipt(ctx, identity, "runtime-start-container", request.RequestId, request, guestRequest, "runner", func() (*hostv1.ContainerResponse, error) {
				t.Fatal("successful replay sent Guest start")
				return nil, nil
			}, func(store.Operation) (store.Operation, bool, error) {
				t.Fatal("successful replay reran current preflight")
				return store.Operation{}, false, nil
			})
			if err != nil || !proto.Equal(first, replay) {
				t.Fatal(replay, err)
			}
			intent, err := database.GetManagedContainer(ctx, "environment", "runner")
			if err != nil || intent.DesiredRunning {
				t.Fatal("historical replay superseded newer Stop", intent, err)
			}
		})
	}
}

func admitOperation(server *agentServer) func(store.Operation) (store.Operation, bool, error) {
	return func(operation store.Operation) (store.Operation, bool, error) {
		return server.controller.store.PutOperation(context.Background(), operation)
	}
}

func TestNewContainerStartIntentSurvivesLostReplyAndHostRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := openTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	server := &agentServer{controller: &Controller{store: database}}
	identity := auth.Identity{EnvironmentID: "environment", NamespaceID: "namespace", SessionID: "first-host"}
	request := &hostv1.ContainerMutationRequest{RequestId: "initial", ContainerId: "runner", ExpectedGenerationId: "generation"}
	guestRequest := &vmmdv1.StartContainerRequest{RequestId: request.RequestId, ActorNamespaceId: identity.NamespaceID, ContainerId: request.ContainerId, ExpectedGenerationId: request.ExpectedGenerationId}
	current := &vmmdv1.Container{Id: "runner", GenerationId: "generation", InstanceId: "instance-1", StartRequestInstanceId: "instance-1", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, StartedAtUnixNano: 1}
	starts := 0
	execute := func() (*hostv1.ContainerResponse, error) {
		starts++
		return &hostv1.ContainerResponse{Container: containerFromGuest(current)}, nil
	}
	first, err := server.containerMutationReceipt(t.Context(), identity, "runtime-start-container", request.RequestId, request, guestRequest, request.ContainerId, execute, admitOperation(server))
	if err != nil {
		t.Fatal(err)
	}
	current.State, current.InstanceId = vmmdv1.ContainerState_CONTAINER_STATE_STOPPED, ""
	stopped := containerFromGuest(current)
	if stopped.GetInstanceId() != "" || stopped.GetStartRequestInstanceId() != "instance-1" {
		t.Fatalf("fresh stopped observation lost the previous Start: %v", stopped)
	}
	old, err := server.containerMutationReceipt(t.Context(), identity, "runtime-start-container", request.RequestId, request, guestRequest, request.ContainerId, execute, admitOperation(server))
	if err != nil || !proto.Equal(old, first) || starts != 1 {
		t.Fatalf("stopped container changed the old receipt: %v, starts=%d, %v", old, starts, err)
	}

	request = proto.Clone(request).(*hostv1.ContainerMutationRequest)
	request.RequestId = "restart-after-" + stopped.GetStartRequestInstanceId()
	guestRequest = proto.Clone(guestRequest).(*vmmdv1.StartContainerRequest)
	guestRequest.RequestId = request.RequestId
	guest := &workloadGuest{states: map[string]*vmmdv1.Container{identity.NamespaceID: current}}
	client := workloadGuestClient(t, guest)
	lostReply := func() (*hostv1.ContainerResponse, error) {
		starts++
		current.InstanceId, current.StartRequestInstanceId = "instance-2", "instance-2"
		current.State, current.StartedAtUnixNano = vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, 2
		current.StartRequestSha256 = hex.EncodeToString(runtimeRequestHash(guestRequest))
		return nil, status.Error(codes.Unavailable, "Guest reply lost after successful Start")
	}
	if _, err := server.containerMutationReceipt(t.Context(), identity, "runtime-start-container", request.RequestId, request, guestRequest, request.ContainerId, lostReply, admitOperation(server)); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	server.controller.store = database
	identity.SessionID = "replacement-host"
	key := sha256.Sum256([]byte("runtime-start-container\x00" + request.RequestId))
	accepted, err := database.GetOperation(t.Context(), "runtime-"+hex.EncodeToString(key[:]))
	if err != nil {
		t.Fatal(err)
	}
	settled, err := server.resolveContainerMutation(t.Context(), client, identity, "runtime-start-container", guestRequest, request.ContainerId, accepted)
	if err != nil || settled.GetContainer().GetStartRequestInstanceId() != "instance-2" {
		t.Fatalf("new intent lost its successful Guest instance: %v, %v", settled, err)
	}
	replay, err := server.containerMutationReceipt(t.Context(), identity, "runtime-start-container", request.RequestId, request, guestRequest, request.ContainerId, lostReply, admitOperation(server))
	if err != nil || !proto.Equal(settled, replay) || starts != 2 {
		t.Fatalf("new intent executed twice: %v, starts=%d, %v", replay, starts, err)
	}
}
