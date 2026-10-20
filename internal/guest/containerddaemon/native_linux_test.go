//go:build linux

package containerddaemon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	bolt "go.etcd.io/bbolt"
)

func TestNativeRetirementPreservesVolumesOverlayAndOtherOwners(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		name := "owned-only"
		if foreign {
			name = "foreign-retained"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			containerdRoot := filepath.Join(root, "containerd")
			for path, value := range map[string]string{
				"containerd/native/snapshots/1/fs/base":                              "native-cache",
				"containerd/io.containerd.snapshotter.v1.native/snapshots/2/fs/base": "native-cache",
				"containerd/overlayfs/snapshots/3/fs/work":                           "workload",
				"volumes/system.controller/private/key":                              "private",
				"volumes/workload/home/value":                                        "user-value",
			} {
				full := filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			database := filepath.Join(containerdRoot, "io.containerd.metadata.v1.bolt", "meta.db")
			if err := os.MkdirAll(filepath.Dir(database), 0700); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(database, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				version, err := tx.CreateBucket([]byte("v1"))
				if err != nil {
					return err
				}
				namespace, err := version.CreateBucket([]byte("runtime"))
				if err != nil {
					return err
				}
				labels, _ := namespace.CreateBucket([]byte("labels"))
				labels.Put([]byte(namespaceconfig.LabelPrefix+"role"), []byte("runtime-internal"))
				containers, _ := namespace.CreateBucket([]byte("containers"))
				for id, snapshotter := range map[string]string{"system": "native", "builder": "native", "workload": "overlayfs"} {
					record, _ := containers.CreateBucket([]byte(id))
					record.Put([]byte("snapshotter"), []byte(snapshotter))
					labels, _ := record.CreateBucket([]byte("labels"))
					if id == "system" {
						labels.Put([]byte(namespaceconfig.LabelPrefix+"owner-namespace"), []byte("system.controller"))
					}
					if id == "builder" {
						labels.Put([]byte(namespaceconfig.LabelPrefix+"internal-build"), []byte("true"))
					}
				}
				if foreign {
					record, _ := containers.CreateBucket([]byte("foreign"))
					record.Put([]byte("snapshotter"), []byte("native"))
				}
				snapshots, _ := namespace.CreateBucket([]byte("snapshots"))
				snapshots.CreateBucket([]byte("native"))
				snapshots.CreateBucket([]byte("overlayfs"))
				images, _ := namespace.CreateBucket([]byte("images"))
				image, _ := images.CreateBucket([]byte("active-system"))
				imageLabels, _ := image.CreateBucket([]byte("labels"))
				imageLabels.Put([]byte("containerd.io/gc.ref.snapshot.native"), []byte("old-chain"))
				imageLabels.Put([]byte("containerd.io/gc.ref.snapshot.overlayfs"), []byte("new-chain"))
				image.Put([]byte("receipt"), []byte("successful-input"))
				return nil
			})
			db.Close()
			if err != nil {
				t.Fatal(err)
			}
			required, err := NativeRetirementRequired(containerdRoot)
			if err != nil || !required {
				t.Fatalf("retirement required=%v %v", required, err)
			}
			if err := RemoveRetiredNative(containerdRoot); err != nil {
				t.Fatal(err)
			}
			if err := RemoveRetiredNative(containerdRoot); err != nil {
				t.Fatal(err)
			}
			err = withNativeMetadata(containerdRoot, false, func(tx *bolt.Tx) error {
				namespace := tx.Bucket([]byte("v1")).Bucket([]byte("runtime"))
				containers := namespace.Bucket([]byte("containers"))
				if containers.Bucket([]byte("system")) != nil || containers.Bucket([]byte("builder")) != nil || containers.Bucket([]byte("workload")) == nil {
					t.Fatal("wrong container retirement scope")
				}
				snapshots := namespace.Bucket([]byte("snapshots"))
				if snapshots.Bucket([]byte("overlayfs")) == nil || (snapshots.Bucket([]byte("native")) != nil) != foreign {
					t.Fatal("wrong snapshot retirement scope")
				}
				image := namespace.Bucket([]byte("images")).Bucket([]byte("active-system"))
				if string(image.Get([]byte("receipt"))) != "successful-input" || string(image.Bucket([]byte("labels")).Get([]byte("containerd.io/gc.ref.snapshot.overlayfs"))) != "new-chain" {
					t.Fatal("image receipt or overlay reference changed")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"volumes/system.controller/private/key", "volumes/workload/home/value", "containerd/overlayfs/snapshots/3/fs/work"} {
				if _, err := os.Stat(filepath.Join(root, path)); err != nil {
					t.Fatalf("owned data lost: %s %v", path, err)
				}
			}
			_, err = os.Stat(filepath.Join(containerdRoot, "native"))
			if foreign && err != nil || !foreign && !os.IsNotExist(err) {
				t.Fatalf("native cache scope: %v", err)
			}
			required, err = NativeRetirementRequired(containerdRoot)
			if err != nil || required {
				t.Fatalf("retired owned containers recur: %v %v", required, err)
			}
		})
	}
}
