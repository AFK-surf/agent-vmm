package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestManagedContainerIntentSurvivesReopenWithoutExportsAndStopsWithOwner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	for _, id := range []string{"service", "job"} {
		if _, err := database.CreateEnvironment(ctx, Environment{ID: id, NamespaceID: id, DisplayName: id, OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "service-owner", EnvironmentID: "service", AdapterID: "agentd-service", DisplayName: "Service", Scope: 1, ConfigTarget: "image", DesiredState: 1, ConfigState: 8, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := database.CreateAgentIntegration(ctx, AgentIntegration{ID: "other-owner", EnvironmentID: "service", AdapterID: "other-service", DisplayName: "Other", Scope: 1, ConfigTarget: "other", DesiredState: 1, ConfigState: 2, PrepareRequestID: "other-prepare"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"worker", "web"} {
		if err := database.SetManagedContainerRunning(ctx, "service", id, "generation-"+id, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.SetManagedContainerRunning(ctx, "job", "job", "generation-job", true); err != nil {
		t.Fatal(err)
	}
	if _, err := database.GetManagedContainer(ctx, "job", "job"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("ordinary job acquired managed intent: %v", err)
	}
	if err := database.SetManagedContainerRunning(ctx, "service", "worker", "generation-worker", true); err != nil {
		t.Fatal(err)
	}
	worker, err := database.GetManagedContainer(ctx, "service", "worker")
	if err != nil || worker.Revision != 1 {
		t.Fatal(worker, err)
	}
	if err := database.SetManagedContainerRunning(ctx, "service", "web", "generation-web", false); err != nil {
		t.Fatal(err)
	}
	database.Close()
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	managed, err := database.ListManagedContainers(ctx)
	if err != nil || len(managed) != 2 {
		t.Fatal(managed, err)
	}
	if managed[0].ContainerID != "web" || managed[0].DesiredRunning || managed[1].ContainerID != "worker" || !managed[1].DesiredRunning {
		t.Fatalf("reopened intents=%+v", managed)
	}
	if exports, err := database.ListServiceExports(ctx, "service"); err != nil || len(exports) != 0 {
		t.Fatal(exports, err)
	}
	owner, err = database.CommitAgentIntegration(ctx, "commit-managed_container_test-68", owner.ID, owner.Revision, "configuration", 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginAgentIntegrationRemoval(ctx, "remove", owner.ID, owner.Revision); err != nil {
		t.Fatal(err)
	}
	worker, err = database.GetManagedContainer(ctx, "service", "worker")
	if err != nil || !worker.DesiredRunning {
		t.Fatal("remaining integration lost service intent", worker, err)
	}
	if managed, err := database.ListManagedContainers(ctx); err != nil || len(managed) != 2 {
		t.Fatal("shared environment duplicated or lost managed containers", managed, err)
	}
	database.Close()
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.BeginAgentIntegrationRemoval(ctx, "remove-last", other.ID, other.Revision); err != nil {
		t.Fatal(err)
	}
	worker, err = database.GetManagedContainer(ctx, "service", "worker")
	if err != nil || worker.DesiredRunning {
		t.Fatal(worker, err)
	}
	if err := database.SetManagedContainerRunning(ctx, "service", "worker", "generation-worker", true); err != nil {
		t.Fatal(err)
	}
	worker, err = database.GetManagedContainer(ctx, "service", "worker")
	if err != nil || worker.DesiredRunning {
		t.Fatal("removed owner resumed intent", worker, err)
	}
	if managed, err := database.ListManagedContainers(ctx); err != nil || len(managed) != 0 {
		t.Fatal(managed, err)
	}
}
