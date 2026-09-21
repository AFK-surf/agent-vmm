package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
)

var (
	ErrInstallationRevisionConflict = errors.New("installation revision conflict")
	ErrResourceCapacityExceeded     = errors.New("host resource capacity exceeded")
	ErrArtifactReplay               = errors.New("appliance artifact was already staged")
	ErrApplianceDomainNotEmpty      = errors.New("appliance domain still owns environments")
	ErrOperationReceiptConflict     = errors.New("appliance operation receipt conflict")
	ErrBundledArtifactConflict      = errors.New("bundled appliance artifact conflicts with durable state")
	ErrApplianceOperationInProgress = errors.New("appliance operation is in progress")
	ErrBundledInstallationConflict  = errors.New("bundled appliance installation changed")
)

type ResourceAmount struct {
	PIDs      uint64
	DiskBytes uint64
}

type ApplianceInstallation struct {
	ID, ProductID, ArtifactID, RegistrationID, EncryptedVolumeID                  string
	EncryptedVolumeIdentity, BootstrapID, CurrentBootID, CurrentContainerInstance string
	DataVolumeBytes                                                               uint64
	DesiredEnabled                                                                bool
	State                                                                         int32
	Revision                                                                      uint64
	CreatedAt, UpdatedAt                                                          time.Time
}

type ApplianceArtifact struct {
	ID, ImageDigest, ImageReference, Platform, ProductID, Release string
	CanonicalHash, Manifest                                       []byte
	CreatedAt                                                     time.Time
}

type ApplianceVolume struct {
	InstallationID, VolumeID, Identity string
	Bytes                              uint64
	Layout                             int32
	UpdatedAt                          time.Time
}

const (
	ApplianceVolumeLayoutHostBlock  int32 = 1
	ApplianceVolumeLayoutGuestImage int32 = 2
)

type ApplianceFrontend struct {
	Name                                        string
	Transport, Exposure                         int32
	MaxFrameBytes, DeadlineSeconds, IdleSeconds uint32
	MaxTotalBytes                               uint64
	MaxConcurrentStreams, MaxUseCount           uint32
}

type ApplianceGrant struct {
	InstallationID            string
	Revision                  uint64
	System, PerEnvironment    ResourceAmount
	EgressMode, MaxEgressMode int32
	DynamicHostLoopback       HostLoopbackFrontendCapacity
}

type HostLoopbackFrontendCapacity struct {
	MaxActiveLeases, MaxActiveLeasesPerAllocation, MaxLeaseSeconds  uint32
	MaxConcurrentConnections, MaxConcurrentConnectionsPerLease      uint32
	MaxFrameBytes, ConnectionDeadlineSeconds, ConnectionIdleSeconds uint32
	MaxConnectionBytes                                              uint64
	MaxUseCount                                                     uint32
}

type ApplianceOperation struct {
	ID, InstallationID, RequestID, Stage, ErrorReason, ErrorMessage string
	Receipt                                                         []byte
	Kind, State, Phase, TerminalReason                              int32
	ExpectedRevision                                                uint64
	DeadlineUnixMillis                                              int64
	CreatedAt, UpdatedAt                                            time.Time
}

type ApplianceOperationTransition struct {
	ExpectedState, ExpectedPhase, State, Phase, TerminalReason int32
	Stage, ErrorReason, ErrorMessage                           string
	Receipt                                                    []byte
}

type ApplianceRemove struct {
	InstallationID, RegistrationID, OperationID string
	ExpectedRevision, CommitRevision            uint64
	ExpectedState, PendingState, ExpectedPhase  int32
	Operation                                   ApplianceOperation
	Receipt                                     []byte
}

func (store *Store) StageApplianceArtifact(ctx context.Context, artifact ApplianceArtifact, operation ApplianceOperation) (ApplianceArtifact, ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceArtifact{}, ApplianceOperation{}, err
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(ctx, `INSERT INTO appliance_artifacts(artifact_id, canonical_hash, image_digest,
		image_reference, platform, product_id, release, manifest,
		created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, artifact.ID,
		applianceBlob(artifact.CanonicalHash), artifact.ImageDigest, artifact.ImageReference, artifact.Platform,
		artifact.ProductID, artifact.Release, applianceBlob(artifact.Manifest), now)
	if err != nil {
		return ApplianceArtifact{}, ApplianceOperation{}, err
	}
	if err := insertApplianceOperation(ctx, tx, operation, now); err != nil {
		return ApplianceArtifact{}, ApplianceOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApplianceArtifact{}, ApplianceOperation{}, err
	}
	staged, err := store.GetApplianceArtifact(ctx, artifact.ID)
	if err != nil {
		return ApplianceArtifact{}, ApplianceOperation{}, err
	}
	created, err := store.GetApplianceOperation(ctx, operation.ID)
	return staged, created, err
}

func (store *Store) GetApplianceArtifact(ctx context.Context, artifactID string) (ApplianceArtifact, error) {
	return scanApplianceArtifact(store.db.QueryRowContext(ctx, applianceArtifactSelect+" WHERE artifact_id=?", artifactID))
}

func (store *Store) GetApplianceArtifactByCanonicalHash(ctx context.Context, hash []byte) (ApplianceArtifact, error) {
	return scanApplianceArtifact(store.db.QueryRowContext(ctx, applianceArtifactSelect+" WHERE canonical_hash=?", applianceBlob(hash)))
}

// SwitchBundledApplianceArtifact atomically records a verified bundled
// artifact and moves one exact desired installation to its stopped revision.
// Domain identity, grant, registration, and volume remain unchanged.
func (store *Store) SwitchBundledApplianceArtifact(ctx context.Context, installationID string, expectedRevision uint64, expectedArtifactID string, expectedState, stoppedState int32, artifact ApplianceArtifact) (ApplianceInstallation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	defer tx.Rollback()
	installation, err := scanApplianceInstallation(tx.QueryRowContext(ctx, applianceInstallationSelect+" WHERE id=?", installationID))
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if installation.Revision != expectedRevision || installation.ArtifactID != expectedArtifactID || installation.State != expectedState ||
		!installation.DesiredEnabled || installation.ProductID != artifact.ProductID || !bundledApplianceUpgradeState(installation.State) {
		return ApplianceInstallation{}, ErrBundledInstallationConflict
	}
	current, err := scanApplianceArtifact(tx.QueryRowContext(ctx, applianceArtifactSelect+" WHERE canonical_hash=?", applianceBlob(artifact.CanonicalHash)))
	if errors.Is(err, sql.ErrNoRows) {
		now := time.Now().UnixNano()
		_, err = tx.ExecContext(ctx, `INSERT INTO appliance_artifacts(artifact_id, canonical_hash, image_digest,
			image_reference, platform, product_id, release, manifest, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, artifact.ID, applianceBlob(artifact.CanonicalHash),
			artifact.ImageDigest, artifact.ImageReference, artifact.Platform, artifact.ProductID, artifact.Release,
			applianceBlob(artifact.Manifest), now)
		current = artifact
	} else if err == nil && !sameApplianceArtifact(current, artifact) {
		return ApplianceInstallation{}, ErrBundledArtifactConflict
	}
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if installation.ArtifactID == current.ID {
		return installation, nil
	}
	var running int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM appliance_operations WHERE installation_id=? AND state=1`, installationID).Scan(&running); err != nil {
		return ApplianceInstallation{}, err
	}
	if running != 0 {
		return ApplianceInstallation{}, ErrApplianceOperationInProgress
	}
	result, err := tx.ExecContext(ctx, `UPDATE appliance_installations SET artifact_id=?, state=?,
		current_boot_id='', current_container_instance='', revision=revision+1, updated_at=?
		WHERE id=? AND revision=? AND artifact_id=? AND desired_enabled=1 AND state=?`, current.ID, stoppedState,
		time.Now().UnixNano(), installationID, expectedRevision, expectedArtifactID, expectedState)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceInstallation{}, err
		}
		return ApplianceInstallation{}, ErrBundledInstallationConflict
	}
	if err := tx.Commit(); err != nil {
		return ApplianceInstallation{}, err
	}
	return store.GetApplianceInstallation(ctx, installationID)
}

func bundledApplianceUpgradeState(state int32) bool {
	return state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING) ||
		state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED)
}

func sameApplianceArtifact(left, right ApplianceArtifact) bool {
	return left.ImageDigest == right.ImageDigest && left.ImageReference == right.ImageReference &&
		left.Platform == right.Platform && left.ProductID == right.ProductID && left.Release == right.Release &&
		bytes.Equal(left.CanonicalHash, right.CanonicalHash) && bytes.Equal(left.Manifest, right.Manifest)
}

const applianceArtifactSelect = `SELECT artifact_id, canonical_hash, image_digest, image_reference, platform,
	product_id, release, manifest, created_at FROM appliance_artifacts`

func scanApplianceArtifact(row rowScanner) (ApplianceArtifact, error) {
	var artifact ApplianceArtifact
	var created int64
	err := row.Scan(&artifact.ID, &artifact.CanonicalHash, &artifact.ImageDigest, &artifact.ImageReference,
		&artifact.Platform, &artifact.ProductID, &artifact.Release, &artifact.Manifest, &created)
	artifact.CreatedAt = time.Unix(0, created)
	return artifact, err
}

const applianceInstallationSelect = `SELECT id, product_id, artifact_id, desired_enabled,
	state, revision, COALESCE(registration_id, ''), encrypted_volume_id, encrypted_volume_identity,
	data_volume_bytes, bootstrap_id, current_boot_id, current_container_instance, created_at, updated_at FROM appliance_installations`

func (store *Store) GetApplianceInstallation(ctx context.Context, id string) (ApplianceInstallation, error) {
	return scanApplianceInstallation(store.db.QueryRowContext(ctx, applianceInstallationSelect+" WHERE id=?", id))
}

func (store *Store) ListApplianceInstallations(ctx context.Context) ([]ApplianceInstallation, error) {
	rows, err := store.db.QueryContext(ctx, applianceInstallationSelect+" ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []ApplianceInstallation
	for rows.Next() {
		value, err := scanApplianceInstallation(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (store *Store) ListApplianceInstallationPage(ctx context.Context, after string, limit int) ([]ApplianceInstallation, error) {
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	rows, err := store.db.QueryContext(ctx, applianceInstallationSelect+" WHERE id>? ORDER BY id LIMIT ?", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []ApplianceInstallation
	for rows.Next() {
		value, err := scanApplianceInstallation(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanApplianceInstallation(row rowScanner) (ApplianceInstallation, error) {
	var value ApplianceInstallation
	var created, updated int64
	err := row.Scan(&value.ID, &value.ProductID, &value.ArtifactID,
		&value.DesiredEnabled, &value.State, &value.Revision, &value.RegistrationID,
		&value.EncryptedVolumeID, &value.EncryptedVolumeIdentity, &value.DataVolumeBytes, &value.BootstrapID,
		&value.CurrentBootID, &value.CurrentContainerInstance, &created, &updated)
	value.CreatedAt, value.UpdatedAt = time.Unix(0, created), time.Unix(0, updated)
	return value, err
}

func (store *Store) UpdateApplianceInstallation(ctx context.Context, value ApplianceInstallation, expectedRevision uint64) (ApplianceInstallation, error) {
	result, err := store.db.ExecContext(ctx, `UPDATE appliance_installations SET desired_enabled=?, state=?,
		registration_id=NULLIF(?, ''), bootstrap_id=?, current_boot_id=?, current_container_instance=?, revision=revision+1,
		updated_at=? WHERE id=? AND revision=?`, value.DesiredEnabled, value.State, value.RegistrationID,
		value.BootstrapID, value.CurrentBootID, value.CurrentContainerInstance, time.Now().UnixNano(), value.ID, expectedRevision)
	if err != nil {
		return ApplianceInstallation{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return ApplianceInstallation{}, err
		}
		return ApplianceInstallation{}, ErrInstallationRevisionConflict
	}
	return store.GetApplianceInstallation(ctx, value.ID)
}

func (store *Store) GetApplianceVolume(ctx context.Context, installationID string) (ApplianceVolume, error) {
	var value ApplianceVolume
	var updated int64
	err := store.db.QueryRowContext(ctx, `SELECT installation_id, volume_id, identity, bytes, layout, updated_at
		FROM appliance_volumes WHERE installation_id=?`, installationID).Scan(&value.InstallationID,
		&value.VolumeID, &value.Identity, &value.Bytes, &value.Layout, &updated)
	value.UpdatedAt = time.Unix(0, updated)
	return value, err
}

func (store *Store) MarkApplianceVolumeGuestImage(ctx context.Context, installationID, volumeID string) error {
	result, err := store.db.ExecContext(ctx, `UPDATE appliance_volumes SET layout=?, updated_at=?
		WHERE installation_id=? AND volume_id=? AND layout=?`, ApplianceVolumeLayoutGuestImage,
		time.Now().UnixNano(), installationID, volumeID, ApplianceVolumeLayoutHostBlock)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	volume, err := store.GetApplianceVolume(ctx, installationID)
	if err == nil && volume.VolumeID == volumeID && volume.Layout == ApplianceVolumeLayoutGuestImage {
		return nil
	}
	return ErrInstallationRevisionConflict
}

func (store *Store) ListApplianceFrontends(ctx context.Context, installationID string) ([]ApplianceFrontend, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT name, transport, exposure, max_frame_bytes,
		max_total_bytes, deadline_seconds, idle_seconds, max_concurrent_streams, max_use_count
		FROM appliance_frontends WHERE installation_id=? ORDER BY name`, installationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []ApplianceFrontend
	for rows.Next() {
		var value ApplianceFrontend
		if err := rows.Scan(&value.Name, &value.Transport, &value.Exposure, &value.MaxFrameBytes,
			&value.MaxTotalBytes, &value.DeadlineSeconds, &value.IdleSeconds,
			&value.MaxConcurrentStreams, &value.MaxUseCount); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (store *Store) GetApplianceGrant(ctx context.Context, installationID string) (ApplianceGrant, error) {
	var grant ApplianceGrant
	err := store.db.QueryRowContext(ctx, applianceGrantSelect+" WHERE installation_id=?", installationID).Scan(applianceGrantScan(&grant)...)
	return grant, err
}

const applianceGrantSelect = `SELECT installation_id, revision,
	system_pids, system_disk_bytes, per_env_pids,
	per_env_disk_bytes, egress_mode, max_egress_mode,
	dynamic_max_active_leases, dynamic_max_active_leases_per_allocation, dynamic_max_lease_seconds,
	dynamic_max_concurrent_connections, dynamic_max_concurrent_connections_per_lease, dynamic_max_frame_bytes,
	dynamic_max_connection_bytes, dynamic_connection_deadline_seconds, dynamic_connection_idle_seconds,
	dynamic_max_use_count FROM appliance_grants`

func applianceGrantScan(grant *ApplianceGrant) []any {
	return []any{&grant.InstallationID, &grant.Revision,
		&grant.System.PIDs, &grant.System.DiskBytes, &grant.PerEnvironment.PIDs,
		&grant.PerEnvironment.DiskBytes, &grant.EgressMode, &grant.MaxEgressMode,
		&grant.DynamicHostLoopback.MaxActiveLeases,
		&grant.DynamicHostLoopback.MaxActiveLeasesPerAllocation, &grant.DynamicHostLoopback.MaxLeaseSeconds,
		&grant.DynamicHostLoopback.MaxConcurrentConnections,
		&grant.DynamicHostLoopback.MaxConcurrentConnectionsPerLease, &grant.DynamicHostLoopback.MaxFrameBytes,
		&grant.DynamicHostLoopback.MaxConnectionBytes, &grant.DynamicHostLoopback.ConnectionDeadlineSeconds,
		&grant.DynamicHostLoopback.ConnectionIdleSeconds, &grant.DynamicHostLoopback.MaxUseCount}
}

func applianceGrantArgs(grant ApplianceGrant, now int64) []any {
	return []any{grant.InstallationID, grant.Revision,
		grant.System.PIDs, grant.System.DiskBytes, grant.PerEnvironment.PIDs,
		grant.PerEnvironment.DiskBytes, grant.EgressMode, grant.MaxEgressMode,
		grant.DynamicHostLoopback.MaxActiveLeases, grant.DynamicHostLoopback.MaxActiveLeasesPerAllocation,
		grant.DynamicHostLoopback.MaxLeaseSeconds, grant.DynamicHostLoopback.MaxConcurrentConnections,
		grant.DynamicHostLoopback.MaxConcurrentConnectionsPerLease, grant.DynamicHostLoopback.MaxFrameBytes,
		grant.DynamicHostLoopback.MaxConnectionBytes, grant.DynamicHostLoopback.ConnectionDeadlineSeconds,
		grant.DynamicHostLoopback.ConnectionIdleSeconds, grant.DynamicHostLoopback.MaxUseCount, now}
}

func (store *Store) InstallApplianceDomain(ctx context.Context, installation ApplianceInstallation,
	grant ApplianceGrant,
	registration RemoteRegistration, frontends []ApplianceFrontend, operation ApplianceOperation) (ApplianceInstallation, RemoteRegistration, ApplianceOperation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	defer tx.Rollback()
	if installation.Revision != 1 || operation.ExpectedRevision != 0 || registration.InstallationID != installation.ID {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	var artifactProduct string
	if err := tx.QueryRowContext(ctx, `SELECT product_id FROM appliance_artifacts
		WHERE artifact_id=?`, installation.ArtifactID).Scan(&artifactProduct); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	if artifactProduct != installation.ProductID {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, ErrInstallationRevisionConflict
	}
	now := time.Now().UnixNano()
	if registration.InventoryRevision == 0 {
		registration.InventoryRevision = 1
	}
	// The installation's registration_id is an immediate foreign key. Create
	// the independent registration row before linking it from the installation.
	if _, err := tx.ExecContext(ctx, `INSERT INTO remote_registrations(id, endpoint, display_name,
		protocol_version, per_env_pids, per_env_disk_bytes,
		max_egress_mode, stop_grace_seconds, state,
		connection_state, enrollment_state, connector_phase, connector_observed_at, enrollment_request_id,
		owner_kind, installation_id, inventory_revision, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		registration.ID, registration.Endpoint, registration.DisplayName, registration.ProtocolVersion,
		registration.PerEnvPIDs, registration.PerEnvDiskBytes, registration.MaxEgressMode,
		registration.StopGraceSeconds,
		registration.State, registration.ConnectionState, registration.EnrollmentState, registration.ConnectorPhase,
		now, registration.EnrollmentRequestID, registration.OwnerKind, registration.InstallationID,
		registration.InventoryRevision, now, now); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO appliance_installations(id, product_id,
		artifact_id, desired_enabled, state, revision, registration_id, encrypted_volume_id,
		encrypted_volume_identity, data_volume_bytes, bootstrap_id, current_boot_id, current_container_instance,
		created_at, updated_at) VALUES (?, ?, ?, 0, ?, 1, ?, ?, ?, ?, ?, '', '', ?, ?)`, installation.ID,
		installation.ProductID, installation.ArtifactID, installation.State,
		registration.ID, installation.EncryptedVolumeID, installation.EncryptedVolumeIdentity,
		installation.DataVolumeBytes, installation.BootstrapID, now, now)
	if err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO appliance_volumes(installation_id, volume_id, identity,
		bytes, layout, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, installation.ID, installation.EncryptedVolumeID,
		installation.EncryptedVolumeIdentity, installation.DataVolumeBytes, ApplianceVolumeLayoutGuestImage, now); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	grant.InstallationID = installation.ID
	if _, err := tx.ExecContext(ctx, `INSERT INTO appliance_grants(installation_id, revision,
		system_pids, system_disk_bytes, per_env_pids, per_env_disk_bytes,
		egress_mode, max_egress_mode, dynamic_max_active_leases,
		dynamic_max_active_leases_per_allocation, dynamic_max_lease_seconds,
		dynamic_max_concurrent_connections, dynamic_max_concurrent_connections_per_lease,
		dynamic_max_frame_bytes, dynamic_max_connection_bytes, dynamic_connection_deadline_seconds,
		dynamic_connection_idle_seconds, dynamic_max_use_count, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, applianceGrantArgs(grant, now)...); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	for _, frontend := range frontends {
		if _, err := tx.ExecContext(ctx, `INSERT INTO appliance_frontends(installation_id, name,
			transport, exposure, max_frame_bytes, max_total_bytes, deadline_seconds, idle_seconds,
			max_concurrent_streams, max_use_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			installation.ID, frontend.Name, frontend.Transport, frontend.Exposure, frontend.MaxFrameBytes,
			frontend.MaxTotalBytes, frontend.DeadlineSeconds, frontend.IdleSeconds,
			frontend.MaxConcurrentStreams, frontend.MaxUseCount); err != nil {
			return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
		}
	}
	if err := insertApplianceOperation(ctx, tx, operation, now); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	installed, err := store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	createdRegistration, err := store.GetRemoteRegistration(ctx, registration.ID)
	if err != nil {
		return ApplianceInstallation{}, RemoteRegistration{}, ApplianceOperation{}, err
	}
	createdOperation, err := store.GetApplianceOperation(ctx, operation.ID)
	return installed, createdRegistration, createdOperation, err
}
