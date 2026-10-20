package store

import "context"

type ManagedContainer struct {
	EnvironmentID         string
	ContainerID           string
	ContainerGenerationID string
	DesiredRunning        bool
	Revision              uint64
}

const managedContainerRunningQuery = `INSERT INTO managed_containers(environment_id,container_id,container_generation_id,desired_running,revision)
        SELECT e.id,?,?,?,1 FROM environments e
        WHERE e.id=? AND e.owner_kind=1 AND (NOT ? OR EXISTS(SELECT 1 FROM agent_integrations i WHERE i.environment_id=e.id AND i.desired_state=1 AND i.config_state NOT IN (6,7)))
        ON CONFLICT(environment_id,container_id) DO UPDATE SET container_generation_id=excluded.container_generation_id,
        desired_running=excluded.desired_running,revision=managed_containers.revision+1
        WHERE managed_containers.container_generation_id<>excluded.container_generation_id OR managed_containers.desired_running<>excluded.desired_running`

func (store *Store) PutWorkloadStartOperation(ctx context.Context, operation Operation, container, generation string) (Operation, bool, error) {
	return store.putOperation(ctx, operation, &ManagedContainer{EnvironmentID: operation.EnvironmentID, ContainerID: container, ContainerGenerationID: generation, DesiredRunning: true})
}

func (store *Store) SetManagedContainerRunning(ctx context.Context, environment, container, generation string, running bool) error {
	_, err := store.db.ExecContext(ctx, managedContainerRunningQuery, container, generation, running, environment, running)
	return err
}

func (store *Store) GetManagedContainer(ctx context.Context, environment, container string) (ManagedContainer, error) {
	var value ManagedContainer
	err := store.db.QueryRowContext(ctx, `SELECT environment_id,container_id,container_generation_id,desired_running,revision FROM managed_containers WHERE environment_id=? AND container_id=?`, environment, container).Scan(&value.EnvironmentID, &value.ContainerID, &value.ContainerGenerationID, &value.DesiredRunning, &value.Revision)
	return value, err
}

func (store *Store) ListManagedContainers(ctx context.Context) ([]ManagedContainer, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT m.environment_id,m.container_id,m.container_generation_id,m.desired_running,m.revision FROM managed_containers m WHERE EXISTS(SELECT 1 FROM agent_integrations i WHERE i.environment_id=m.environment_id AND i.desired_state=1 AND i.config_state NOT IN (6,7)) ORDER BY m.environment_id,m.container_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []ManagedContainer
	for rows.Next() {
		var value ManagedContainer
		if err := rows.Scan(&value.EnvironmentID, &value.ContainerID, &value.ContainerGenerationID, &value.DesiredRunning, &value.Revision); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (store *Store) ServiceContainerStoppedByOwner(ctx context.Context, environment, container, generation string) (bool, error) {
	var stopped bool
	err := store.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_containers WHERE environment_id=? AND container_id=? AND container_generation_id=? AND desired_running=0)`, environment, container, generation).Scan(&stopped)
	return stopped, err
}

func (store *Store) DeleteManagedContainer(ctx context.Context, environment, container string) error {
	_, err := store.db.ExecContext(ctx, `DELETE FROM managed_containers WHERE environment_id=? AND container_id=?`, environment, container)
	return err
}
