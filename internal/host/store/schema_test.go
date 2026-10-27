package store

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
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
