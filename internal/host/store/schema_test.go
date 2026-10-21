package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

func TestOpenRejectsUnsupportedSchemaWithoutChangingIt(t *testing.T) {
	if _, err := ReadSchemaVersion(filepath.Join(t.TempDir(), "missing.db")); err != nil {
		t.Fatalf("missing database = %v", err)
	}
	for _, test := range []struct {
		version int
		code    string
		message string
	}{
		{16, SchemaDataRequiresConversion, "host database schema 16 requires offline conversion"},
		{30, SchemaDataRequiresConversion, "host database schema 30 requires offline conversion"},
		{schemaVersion + 1, SchemaDataNewer, fmt.Sprintf("host database schema %d is newer than this Host supports", schemaVersion+1)},
	} {
		t.Run(fmt.Sprint(test.version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "Application Support", "host.db")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			database, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := database.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (?, 0)`, test.version); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			opened, err := Open(path)
			if opened != nil {
				opened.Close()
			}
			var schemaErr *SchemaError
			if !errors.As(err, &schemaErr) || schemaErr.Code != test.code || schemaErr.DataSchema != test.version || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("open error = %v", err)
			}
			if version, err := ReadSchemaVersion(path); err != nil || version != test.version {
				t.Fatalf("read schema = %d %v", version, err)
			}
		})
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

func createMainDatabase(t *testing.T, path string, version int, statements ...string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	migrations := []func(*sql.Tx) error{migrateV1, migrateV2, migrateV3, migrateV4, migrateV5, migrateV6, migrateV9, migrateV10, migrateV16, migrateV19, migrateV20, migrateV21, migrateV22, migrateV23, migrateV24}
	if version == 25 {
		migrations = append(migrations, migrateV25)
	}
	for _, migration := range migrations {
		if err := migration(tx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES(?,1)`, version); err != nil {
		t.Fatal(err)
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func schemaDefinitions(t *testing.T, database *sql.DB) []string {
	t.Helper()
	rows, err := database.Query(`SELECT type||' '||name||' '||sql FROM sqlite_master WHERE sql IS NOT NULL AND name<>'schema_migrations' ORDER BY type,name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var definitions []string
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			t.Fatal(err)
		}
		definitions = append(definitions, definition)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return definitions
}

func TestOpenConvertsMainSchemaPreservingServiceFacts(t *testing.T) {
	fresh, err := Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	expected := schemaDefinitions(t, fresh.db)
	for _, previous := range []int{24, 25} {
		t.Run(fmt.Sprintf("schema%d", previous), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			createMainDatabase(t, path, previous,
				`INSERT INTO environments(id,namespace_id,display_name,cpu_millis,memory_bytes,pids,disk_bytes,egress_mode,created_at,updated_at)
				 VALUES('environment','namespace','local',1000,1000,10,1000,1,1,1)`,
				`INSERT INTO agent_integrations(id,adapter_id,display_name,scope,config_target,environment_id,desired_state,config_state,created_at,updated_at,prepare_request_id)
				 VALUES('integration','adapter','integration',1,'target','environment',1,4,1,1,'prepare')`,
				`INSERT INTO service_exports(export_id,request_id,environment_id,container_id,container_instance_id,port,protocol,revision,created_at,updated_at)
				 VALUES('export','export-request','environment','container','instance',8080,1,3,1,1)`,
				`INSERT INTO service_imports(import_id,request_id,source_environment_id,source_container_id,source_container_instance_id,destination_device_id,destination_export_id,virtual_service_name,revision,created_at,updated_at,destination_port)
				 VALUES('import','import-request','environment','client','client-instance','device','export','service',2,1,1,8080)`,
				`INSERT INTO access_routes(route_id,import_id,route_class,source_device_id,destination_device_id,destination_export_id,state,revision,generation,expires_at,source_environment_id,source_container_instance_id,policy_revision,capability,updated_at)
				 VALUES('route','import',1,'device','device','export',2,4,5,9223372036854775807,'environment','client-instance',1,X'01',1)`,
				`INSERT INTO service_listeners(listener_id,request_id,mesh_id,destination_device_id,destination_export_id,exposure,listen_address,route_id,capability_id,capability,endpoint,expires_at,revision,created_at,updated_at)
				 VALUES('listener','listener-request','mesh','device','export',1,'127.0.0.1:1','route','capability',X'01',X'02',9223372036854775807,6,1,1)`,
				`INSERT INTO appliance_artifacts(artifact_id,canonical_hash,image_digest,image_reference,platform,product_id,release,manifest,created_at)
				 VALUES('artifact',X'01','sha256:digest','registry/image','linux/arm64','product','release',X'02',1)`)
			opened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			if actual := schemaDefinitions(t, opened.db); !slices.Equal(actual, expected) {
				t.Fatalf("converted schema differs from a new schema:\n%s\nwant:\n%s", strings.Join(actual, "\n"), strings.Join(expected, "\n"))
			}
			var version int
			if err := opened.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != schemaVersion {
				t.Fatalf("version=%d err=%v", version, err)
			}
			exported, err := opened.GetServiceExport(t.Context(), "export")
			if err != nil || exported.GetEnvironmentId() != "environment" || exported.GetPort() != 8080 || exported.GetRevision() != 3 || exported.GetContainerGenerationId() != "" {
				t.Fatalf("export changed: %v %v", exported, err)
			}
			imported, err := opened.GetServiceImport(t.Context(), "import")
			if err != nil || imported.GetSourceEnvironmentId() != "environment" || imported.GetDestinationExportId() != "export" || imported.GetRevision() != 2 || imported.GetDestinationFrontend() != nil {
				t.Fatalf("import changed: %v %v", imported, err)
			}
			var routeGeneration, local int
			if err := opened.db.QueryRow(`SELECT generation FROM access_routes WHERE route_id='route' AND import_id='import'`).Scan(&routeGeneration); err != nil || routeGeneration != 5 {
				t.Fatalf("route changed: %d %v", routeGeneration, err)
			}
			if err := opened.db.QueryRow(`SELECT local FROM service_listeners WHERE listener_id='listener' AND destination_export_id='export'`).Scan(&local); err != nil || local != 0 {
				t.Fatalf("listener changed: %d %v", local, err)
			}
			if containers, err := opened.ListManagedContainers(t.Context()); err != nil || len(containers) != 0 {
				t.Fatalf("main services gained running intent: %+v %v", containers, err)
			}
			artifact, err := opened.GetApplianceArtifact(t.Context(), "artifact")
			if err != nil || artifact.ProductID != "product" || string(artifact.Manifest) != "\x02" {
				t.Fatalf("artifact changed: %+v %v", artifact, err)
			}
			if _, err := opened.AllocationDisposal(context.Background(), "registration", "allocation", 1); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("new deletion lookup=%v", err)
			}
		})
	}
}

func TestOpenRejectsEncryptedApplianceStorageWithoutChangingState(t *testing.T) {
	for _, version := range []int{24, 25} {
		for _, layout := range []int{1, 2} {
			t.Run(fmt.Sprintf("schema%d/layout%d", version, layout), func(t *testing.T) {
				directory := t.TempDir()
				path := filepath.Join(directory, "host.db")
				createMainDatabase(t, path, version,
					`INSERT INTO appliance_installations(id,product_id,artifact_id,state,encrypted_volume_id,encrypted_volume_identity,data_volume_bytes,created_at,updated_at)
					 VALUES('installation','product','artifact',1,'volume','luks-identity',1,1,1)`,
					fmt.Sprintf(`INSERT INTO appliance_volumes(installation_id,volume_id,identity,bytes,updated_at,layout) VALUES('installation','volume','luks-identity',1,1,%d)`, layout))
				files := []string{filepath.Join(directory, "state.raw"), filepath.Join(directory, "volume.luks"), filepath.Join(directory, "volume.key")}
				for _, file := range files {
					if err := os.WriteFile(file, []byte("preserved"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				opened, err := Open(path)
				if opened != nil {
					opened.Close()
				}
				var schemaErr *SchemaError
				if !errors.As(err, &schemaErr) || schemaErr.Code != SchemaDataRequiresConversion || schemaErr.DataSchema != version || !strings.Contains(err.Error(), "unsupported encrypted appliance storage") {
					t.Fatalf("open error=%v", err)
				}
				database, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer database.Close()
				var storedVersion, storedLayout int
				var identity string
				if err := database.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&storedVersion); err != nil {
					t.Fatal(err)
				}
				if err := database.QueryRow(`SELECT identity, layout FROM appliance_volumes WHERE installation_id='installation'`).Scan(&identity, &storedLayout); err != nil {
					t.Fatal(err)
				}
				if storedVersion != version || storedLayout != layout || identity != "luks-identity" {
					t.Fatalf("old storage changed: %d %d %s", storedVersion, storedLayout, identity)
				}
				var migrated int
				if err := database.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('appliance_installations') WHERE name='data_volume_id'`).Scan(&migrated); err != nil {
					t.Fatal(err)
				}
				if migrated != 0 {
					t.Fatal("rejected storage was converted")
				}
				for _, file := range files {
					data, err := os.ReadFile(file)
					if err != nil || string(data) != "preserved" {
						t.Fatalf("file %s changed: %v", file, err)
					}
				}
			})
		}
	}
}

func migrateV18Legacy(tx *sql.Tx) error {
	statements := []string{
		`DROP TABLE resource_reservations`,
		`CREATE TABLE resource_reservations (
			id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2, 3)), owner_id TEXT NOT NULL,
			resource_kind INTEGER NOT NULL CHECK(resource_kind IN (1, 2, 3)), resource_id TEXT NOT NULL,
			cpu_millis INTEGER NOT NULL, memory_bytes INTEGER NOT NULL, pids INTEGER NOT NULL, disk_bytes INTEGER NOT NULL,
			retained INTEGER NOT NULL DEFAULT 0 CHECK(retained IN (0, 1)), created_at INTEGER NOT NULL,
			UNIQUE(owner_kind, owner_id, resource_kind, resource_id)
		)`,
		`CREATE TABLE workload_reservations_v2 (
		allocation_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0),
		registration_id TEXT NOT NULL REFERENCES remote_registrations(id) ON DELETE CASCADE,
		environment_id TEXT NOT NULL UNIQUE REFERENCES environments(id) ON DELETE CASCADE,
		cpu_guarantee_millis INTEGER NOT NULL, cpu_max_millis INTEGER NOT NULL, cpu_weight INTEGER NOT NULL,
		memory_high_bytes INTEGER NOT NULL, memory_max_bytes INTEGER NOT NULL, pid_max INTEGER NOT NULL,
		writable_quota_bytes INTEGER NOT NULL, phase INTEGER NOT NULL CHECK(phase BETWEEN 1 AND 5),
		guest_receipt BLOB NOT NULL DEFAULT X'', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
		PRIMARY KEY(allocation_id, generation), UNIQUE(registration_id, allocation_id)
	)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}
