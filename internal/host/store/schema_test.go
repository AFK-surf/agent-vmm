package store

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"modernc.org/sqlite"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestOpenRejectsIncompatibleSchemaWithoutChangingIt(t *testing.T) {
	for _, version := range []int{1, 18, 25, 38, schemaVersion + 1} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.db")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO schema_migrations VALUES(?, 1)`, version); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Open(path)
			var schemaErr *SchemaError
			if !errors.As(err, &schemaErr) || schemaErr.Code != SchemaIncompatible || schemaErr.DataSchema != version {
				t.Fatalf("Open error = %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("rejected database changed: %v", err)
			}
			entries, err := os.ReadDir(filepath.Dir(path))
			if err != nil || len(entries) != 1 {
				t.Fatalf("rejected database created files: %v, %v", entries, err)
			}
		})
	}
}

func TestOpenCurrentSchemaPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`INSERT INTO audit(actor,target,method,grant_id,request_id,result_code,reason,occurred_at) VALUES('operator','local','method','','',0,'',?)`, int64(1<<62)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var actor string
	if err := db.db.QueryRow(`SELECT actor FROM audit`).Scan(&actor); err != nil || actor != "operator" {
		t.Fatalf("actor = %q, %v", actor, err)
	}
	version, err := ReadSchemaVersion(path)
	if err != nil || version != schemaVersion {
		t.Fatalf("schema = %d, %v", version, err)
	}
}

func TestForwardMigrationCommitsFactsAndRetriesAfterFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec(`INSERT INTO environments(id,namespace_id,display_name,pids,disk_bytes,egress_mode,created_at,updated_at) VALUES('env','namespace','original',32,1024,1,1,1)`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	first := `ALTER TABLE environments ADD COLUMN migration_note TEXT NOT NULL DEFAULT ''; UPDATE environments SET migration_note=display_name, display_name='converted'`
	_, err = open(path, []string{first, `INSERT INTO missing_table VALUES(1)`})
	var failure *SchemaError
	if !errors.As(err, &failure) || failure.Code != MigrationFailed || failure.DataSchema != minimumSchema {
		t.Fatalf("failure = %v", err)
	}
	version, err := ReadSchemaVersion(path)
	if err != nil || version != minimumSchema {
		t.Fatalf("rollback schema = %d, %v", version, err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.db.QueryRow(`SELECT display_name FROM environments WHERE id='env'`).Scan(&name); err != nil || name != "original" {
		t.Fatalf("rollback fact = %q, %v", name, err)
	}
	db.Close()
	for range 2 {
		db, err = open(path, []string{first})
		if err != nil {
			t.Fatal(err)
		}
		var namespace, note string
		if err := db.db.QueryRow(`SELECT namespace_id, display_name, migration_note FROM environments WHERE id='env'`).Scan(&namespace, &name, &note); err != nil || namespace != "namespace" || name != "converted" || note != "original" {
			t.Fatalf("migrated facts = %q %q %q, %v", namespace, name, note, err)
		}
		db.Close()
	}
	if _, err := Open(path); !errors.As(err, &failure) || failure.Code != SchemaIncompatible {
		t.Fatalf("older binary accepted committed migration: %v", err)
	}
}

func TestOpenRejectsPopulatedUnversionedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE user_data(value TEXT); INSERT INTO user_data VALUES('keep')`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	var failure *SchemaError
	if !errors.As(err, &failure) || failure.Code != SchemaIncompatible {
		t.Fatalf("unversioned state = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("unversioned state changed: %v", err)
	}
}

func TestMigrationProcessCrashRollsBack(t *testing.T) {
	if path := os.Getenv("AGENT_VMM_MIGRATION_CRASH_DB"); path != "" {
		sqlite.MustRegisterScalarFunction("migration_pause", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			fmt.Println("migration transaction open")
			select {}
		})
		_, err := open(path, []string{`ALTER TABLE environments ADD COLUMN migration_note TEXT; SELECT migration_pause()`})
		t.Fatalf("migration returned before kill: %v", err)
	}
	path := filepath.Join(t.TempDir(), "host.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMigrationProcessCrashRollsBack$")
	child.Env = append(os.Environ(), "AGENT_VMM_MIGRATION_CRASH_DB="+path)
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Wait()
	scanner := bufio.NewScanner(output)
	if !scanner.Scan() || scanner.Text() != "migration transaction open" {
		t.Fatalf("child did not enter migration: %q, %v", scanner.Text(), scanner.Err())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	child.Wait()
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var columns int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('environments') WHERE name='migration_note'`).Scan(&columns); err != nil || columns != 0 {
		t.Fatalf("partial DDL survived crash: %d, %v", columns, err)
	}
	version, err := ReadSchemaVersion(path)
	if err != nil || version != minimumSchema {
		t.Fatalf("crash schema = %d, %v", version, err)
	}
}
