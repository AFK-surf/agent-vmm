package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestHostSocketsAreSetPerPortRemovedAndGoWithTheirEnvironment(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	for _, id := range []string{"a", "b"} {
		if _, err := database.CreateEnvironment(ctx, Environment{ID: id, NamespaceID: id, OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := database.SetHostSocket(ctx, "a", 7480, "/run/one.sock")
	if err != nil || first.Revision != 1 {
		t.Fatalf("set = %+v, %v", first, err)
	}
	if same, err := database.SetHostSocket(ctx, "a", 7480, "/run/one.sock"); err != nil || same.Revision != 1 {
		t.Fatalf("the same path again = %+v, %v", same, err)
	}
	if moved, err := database.SetHostSocket(ctx, "a", 7480, "/run/two.sock"); err != nil || moved.Revision != 2 || moved.Path != "/run/two.sock" {
		t.Fatalf("another path = %+v, %v", moved, err)
	}
	if _, err := database.SetHostSocket(ctx, "b", 7481, "/run/b.sock"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetHostSocket(ctx, "a", 53, "/run/dns.sock"); err == nil {
		t.Fatal("the Host virtual address's DNS port was taken")
	}
	if _, err := database.SetHostSocket(ctx, "missing", 7480, "/run/x.sock"); err == nil {
		t.Fatal("an environment that does not exist got a socket")
	}
	// Kept as the Host restarts.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if database, err = Open(path); err != nil {
		t.Fatal(err)
	}
	all, err := database.ListHostSockets(ctx, "")
	if err != nil || len(all) != 2 || all[0] != (HostSocket{EnvironmentID: "a", Port: 7480, Path: "/run/two.sock", Revision: 2}) || all[1].EnvironmentID != "b" {
		t.Fatalf("list = %+v, %v", all, err)
	}
	// Every environment's, and an environment's own in its place at the same port.
	if _, err := database.SetHostSocket(ctx, "", 7480, "/run/all.sock"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SetHostSocket(ctx, "", 7482, "/run/other.sock"); err != nil {
		t.Fatal(err)
	}
	all, err = database.ListHostSockets(ctx, "")
	if err != nil || len(all) != 4 {
		t.Fatalf("list = %+v, %v", all, err)
	}
	ofA := HostSocketsOf(all, "a")
	if len(ofA) != 2 || ofA[0].Path != "/run/two.sock" || ofA[1].Path != "/run/other.sock" {
		t.Fatalf("what a reaches = %+v", ofA)
	}
	if ofC := HostSocketsOf(all, "c"); len(ofC) != 2 || ofC[0].Path != "/run/all.sock" {
		t.Fatalf("what another environment reaches = %+v", ofC)
	}
	if listed, err := database.ListHostSockets(ctx, "b"); err != nil || len(listed) != 3 {
		t.Fatalf("b's and every environment's = %+v, %v", listed, err)
	}
	if err := database.RemoveHostSocket(ctx, "", 7482); err != nil {
		t.Fatal(err)
	}
	if err := database.RemoveHostSocket(ctx, "", 7480); err != nil {
		t.Fatal(err)
	}
	if err := database.RemoveHostSocket(ctx, "a", 7480); err != nil {
		t.Fatal(err)
	}
	if err := database.RemoveHostSocket(ctx, "a", 7480); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("removing again = %v", err)
	}
	if _, err := database.db.ExecContext(ctx, `DELETE FROM environments WHERE id='b'`); err != nil {
		t.Fatal(err)
	}
	if left, err := database.ListHostSockets(ctx, ""); err != nil || len(left) != 0 {
		t.Fatalf("after the environment went = %+v, %v", left, err)
	}
}

func TestAHostDatabaseFromBeforeHostSocketsGainsThem(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	// Schema 39 as the Host before them wrote it: no host_sockets table.
	before, err := open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := before.db.ExecContext(ctx, `DROP TABLE host_sockets`); err != nil {
		t.Fatal(err)
	}
	if _, err := before.CreateEnvironment(ctx, Environment{ID: "a", NamespaceID: "a", OwnerKind: OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	before.Close()
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if version, err := ReadSchemaVersion(path); err != nil || version != CurrentSchema() {
		t.Fatalf("schema = %d, %v", version, err)
	}
	if _, err := database.SetHostSocket(ctx, "a", 7480, "/run/one.sock"); err != nil {
		t.Fatal(err)
	}
}
