//go:build darwin || linux

package host

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type deleteWorkloadGuest struct {
	recoveryServiceGuest
	deleteError  codes.Code
	acknowledged bool
	inspections  int
}

func (guest *deleteWorkloadGuest) InspectContainer(ctx context.Context, request *vmmdv1.InspectContainerRequest) (*vmmdv1.InspectContainerResponse, error) {
	guest.mu.Lock()
	guest.inspections++
	guest.mu.Unlock()
	return guest.recoveryServiceGuest.InspectContainer(ctx, request)
}

func (guest *deleteWorkloadGuest) DeleteContainer(_ context.Context, request *vmmdv1.DeleteContainerRequest) (*vmmdv1.DeleteContainerResponse, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.deleteError != codes.OK {
		return nil, status.Error(guest.deleteError, "delete failed")
	}
	if guest.acknowledged {
		return &vmmdv1.DeleteContainerResponse{}, nil
	}
	container := guest.containers[request.GetContainerId()]
	if container == nil {
		return nil, status.Error(codes.NotFound, "absent")
	}
	if container.GetGenerationId() != request.GetExpectedGenerationId() {
		return nil, status.Error(codes.Aborted, "generation changed")
	}
	delete(guest.containers, request.GetContainerId())
	guest.acknowledged = true
	return &vmmdv1.DeleteContainerResponse{}, nil
}

func TestWorkloadDeleteSettlesIntentAndProjectionOnlyForConfirmedAbsence(t *testing.T) {
	for _, mode := range []string{"deleted", "already-absent", "delete-failed", "new-generation", "replayed-success", "observation-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			controller, integration, _ := integrationAuthorityFixture(t)
			ctx := t.Context()
			const id = "worker"
			generation := "original"
			if mode == "new-generation" || mode == "replayed-success" {
				generation = "replacement"
			}
			if err := controller.store.SetManagedContainerRunning(ctx, integration.EnvironmentID, id, generation, true); err != nil {
				t.Fatal(err)
			}
			if err := controller.store.SetManagedContainerRunning(ctx, integration.EnvironmentID, "neighbor", "neighbor-generation", false); err != nil {
				t.Fatal(err)
			}
			exported, err := controller.store.CreateServiceExport(ctx, "export", &servicev1.ServiceExport{ExportId: "export", EnvironmentId: integration.EnvironmentID, ContainerId: id, ContainerGenerationId: generation, ContainerInstanceId: "instance", Port: 8080, Protocol: servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP})
			if err != nil {
				t.Fatal(err)
			}
			setTestWorkloadInstance(t, controller, integration.EnvironmentID, id, "instance")
			guest := &deleteWorkloadGuest{recoveryServiceGuest: recoveryServiceGuest{containers: map[string]*vmmdv1.Container{id: {Id: id, GenerationId: generation, State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}}, containerErrors: map[string]codes.Code{}}}
			if mode == "already-absent" {
				delete(guest.containers, id)
			}
			if mode == "delete-failed" {
				guest.deleteError = codes.Unavailable
			}
			if mode == "observation-unavailable" {
				guest.containerErrors[id] = codes.Unavailable
			}
			client := serviceRecoveryClient(t, guest)
			request := &vmmdv1.DeleteContainerRequest{RequestId: "delete-original", ActorNamespaceId: "namespace", ContainerId: id, ExpectedGenerationId: "original"}
			if mode == "replayed-success" {
				guest.mu.Lock()
				guest.containers[id].GenerationId = "original"
				guest.mu.Unlock()
				if _, err := client.DeleteContainer(ctx, request); err != nil {
					t.Fatal(err)
				}
				guest.mu.Lock()
				guest.containers[id] = &vmmdv1.Container{Id: id, GenerationId: "replacement", State: vmmdv1.ContainerState_CONTAINER_STATE_STOPPED}
				guest.mu.Unlock()
			}
			unlock, lockErr := controller.workloads.LockEnvironment(context.Background(), integration.EnvironmentID)
			if lockErr != nil {
				t.Fatal(lockErr)
			}

			err = controller.workloads.DeleteWorkloadContainerLocked(ctx, client, integration.EnvironmentID, request)
			unlock()
			wantCode := codes.OK
			if mode == "delete-failed" || mode == "observation-unavailable" {
				wantCode = codes.Unavailable
			} else if mode == "new-generation" {
				wantCode = codes.Aborted
			}
			if status.Code(err) != wantCode {
				t.Fatalf("delete result=%v want=%v", err, wantCode)
			}
			settled := mode == "deleted" || mode == "already-absent"
			intent, intentErr := controller.store.GetManagedContainer(ctx, integration.EnvironmentID, id)
			if settled {
				if !errors.Is(intentErr, sql.ErrNoRows) || testWorkloadExists(controller, integration.EnvironmentID, id) {
					t.Fatalf("successful deletion retained Host facts: intent=%+v error=%v run=%v", intent, intentErr, testWorkloadInstance(controller, integration.EnvironmentID, id))
				}
				inspections := guest.inspections
				for range 2 {
					if err := controller.services.RestoreManagedServiceContainers(ctx, client); err != nil {
						t.Fatal(err)
					}
				}
				if guest.inspections != inspections {
					t.Fatal("managed recovery kept polling the deleted container")
				}
			} else if intentErr != nil || intent.ContainerGenerationID != generation || !intent.DesiredRunning || !testWorkloadExists(controller, integration.EnvironmentID, id) {
				t.Fatalf("unsettled or old deletion removed current Host fence: intent=%+v error=%v", intent, intentErr)
			}
			current, err := controller.store.GetServiceExport(ctx, exported.GetExportId())
			if err != nil || (settled && (current.GetContainerGenerationId() != "" || current.GetContainerInstanceId() != "")) || (!settled && current.GetContainerInstanceId() != "instance") {
				t.Fatalf("projection did not follow confirmed deletion: %v %v", current, err)
			}
			if _, err := controller.store.GetManagedContainer(ctx, integration.EnvironmentID, "neighbor"); err != nil {
				t.Fatal("neighbor intent was deleted", err)
			}
		})
	}
}
