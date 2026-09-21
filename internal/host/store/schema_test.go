package store

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenConvertsV18AllocationGenerationAndDropsReservationLedgers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database := createV18Database(t, path, true)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	environment, err := opened.GetRemoteEnvironment(t.Context(), "registration-1", "allocation-1")
	if err != nil {
		t.Fatal(err)
	}
	if environment.AllocationGeneration != 7 {
		t.Fatalf("allocation generation=%d want=7", environment.AllocationGeneration)
	}
	var version int
	if err := opened.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version=%d want=%d", version, schemaVersion)
	}
	var obsolete int
	if err := opened.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workload_reservations_v2'`).Scan(&obsolete); err != nil {
		t.Fatal(err)
	}
	if obsolete != 0 {
		t.Fatal("obsolete workload reservation ledger still exists")
	}
	if err := opened.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='resource_reservations'`).Scan(&obsolete); err != nil {
		t.Fatal(err)
	}
	if obsolete != 0 {
		t.Fatal("compute reservation projection still exists")
	}
}

func TestOpenKeepsV18DatabaseIntactWhenAllocationGenerationIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database := createV18Database(t, path, false)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := Open(path)
	if opened != nil {
		opened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "1 remote environments lack an exact reservation") {
		t.Fatalf("missing exact generation error=%v", err)
	}
	database, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var version int
	if err := database.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 18 {
		t.Fatalf("failed conversion changed schema version to %d", version)
	}
	rows, err := database.Query(`PRAGMA table_info(environments)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	newColumn := false
	for rows.Next() {
		var sequence, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&sequence, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		newColumn = newColumn || name == "allocation_generation"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if newColumn {
		t.Fatal("failed conversion committed allocation_generation")
	}
}

func TestOpenConvertsV19ArtifactsWithoutTrustProjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range []func(*sql.Tx) error{migrateV1, migrateV2, migrateV3, migrateV4, migrateV5, migrateV6, migrateV9, migrateV10, migrateV16, migrateV19} {
		if err := migration(tx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES(19, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO appliance_artifacts(
		artifact_id, canonical_hash, image_digest, image_reference, platform, product_id, release, verified, manifest, created_at)
		VALUES('artifact-1', X'01', 'sha256:digest', 'registry/image', 'linux/arm64', 'product', 'release', 0, X'02', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	opened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	artifact, err := opened.GetApplianceArtifact(t.Context(), "artifact-1")
	if err != nil || artifact.ProductID != "product" || string(artifact.Manifest) != "\x02" {
		t.Fatalf("artifact was not preserved: artifact=%+v err=%v", artifact, err)
	}
}

func TestOpenRejectsUnconvertiblePreviousSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (16, 0)`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path)
	if opened != nil {
		opened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "host database schema 16 requires offline conversion") {
		t.Fatalf("previous schema was not rejected with preservation guidance: %v", err)
	}
}

func createV18Database(t *testing.T, path string, includeReservation bool) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range []func(*sql.Tx) error{migrateV1, migrateV2, migrateV3, migrateV4, migrateV5, migrateV6, migrateV9, migrateV10, migrateV16, migrateV18Legacy} {
		if err := migration(tx); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"remote_registrations", "appliance_grants"} {
		for _, column := range []string{"aggregate_cpu_millis", "aggregate_memory_bytes", "aggregate_pids", "aggregate_disk_bytes"} {
			if _, err := tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` INTEGER NOT NULL DEFAULT 1`); err != nil {
				t.Fatal(err)
			}
		}
	}
	now := time.Now().UnixNano()
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version,applied_at) VALUES(18,?)`, now); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO remote_registrations(
		id,endpoint,display_name,protocol_version,max_environments,aggregate_cpu_millis,
		aggregate_memory_bytes,aggregate_pids,aggregate_disk_bytes,per_env_cpu_millis,
		per_env_memory_bytes,per_env_pids,per_env_disk_bytes,max_egress_mode,max_lease_seconds,
		disconnect_grace_seconds,stop_grace_seconds,state,created_at,updated_at)
		VALUES('registration-1','https://controller.invalid','test','1',256,8000,12000000000,
		2048,64000000000,7000,9000000000,2048,8000000000,1,60,60,30,1,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO environments(
		id,namespace_id,display_name,cpu_millis,memory_bytes,pids,disk_bytes,egress_mode,
		owner_kind,registration_id,allocation_id,allocation_state,created_at,updated_at)
		VALUES('environment-1','namespace-1','test',7000,9000000000,2048,8000000000,1,
		2,'registration-1','allocation-1',2,?,?)`, now, now); err != nil {
		t.Fatal(err)
	}
	if includeReservation {
		if _, err := tx.Exec(`INSERT INTO workload_reservations_v2(
			allocation_id,generation,registration_id,environment_id,cpu_guarantee_millis,
			cpu_max_millis,cpu_weight,memory_high_bytes,memory_max_bytes,pid_max,
			writable_quota_bytes,phase,created_at,updated_at)
			VALUES('allocation-1',7,'registration-1','environment-1',1000,7000,100,
			8000000000,9000000000,2048,8000000000,2,?,?)`, now, now); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO resource_reservations(
			id,owner_kind,owner_id,resource_kind,resource_id,cpu_millis,memory_bytes,pids,disk_bytes,retained,created_at)
			VALUES
			('system',1,'host',1,'system',100,100,10,100,0,?),
			('runtime',1,'host',2,'runtime',200,200,20,200,0,?),
			('stale-allocation',3,'registration-1',3,'environment-1',7000,9000000000,2048,8000000000,0,?)`, now, now, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return database
}
