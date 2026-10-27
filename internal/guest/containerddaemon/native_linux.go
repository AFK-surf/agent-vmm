//go:build linux

package containerddaemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AFK-surf/agent-vmm/internal/guest/namespaceconfig"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	bolt "go.etcd.io/bbolt"
)

const nativeSnapshotter = "native"

func retiredNativeOwner(namespace string, labels map[string]string) bool {
	return strings.HasPrefix(namespace, "system.") || strings.HasPrefix(labels[namespaceconfig.LabelPrefix+"owner-namespace"], "system.") || labels[namespaceconfig.LabelPrefix+"internal-build"] == "true"
}

func NativeRetirementRequired(root string) (bool, error) {
	required := false
	err := withNativeMetadata(root, false, func(tx *bolt.Tx) error {
		version := tx.Bucket([]byte("v1"))
		if version == nil {
			return nil
		}
		return version.ForEach(func(name, value []byte) error {
			namespace := version.Bucket(name)
			if namespace == nil {
				return nil
			}
			containers := namespace.Bucket([]byte("containers"))
			if containers != nil {
				if err := containers.ForEach(func(id, value []byte) error {
					record := containers.Bucket(id)
					if record != nil && string(record.Get([]byte("snapshotter"))) == nativeSnapshotter && retiredNativeOwner(string(name), nativeRecordLabels(record)) {
						required = true
					}
					return nil
				}); err != nil {
					return err
				}
			}

			return nil
		})
	})
	return required, err
}

// StopRetiredNativeTasks uses the existing shim task API before offline cache retirement.
func StopRetiredNativeTasks(ctx context.Context, socket string) error {
	client, err := containerd.New(socket)
	if err != nil {
		return err
	}
	defer client.Close()
	names, err := client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		nsctx := namespaces.WithNamespace(ctx, name)
		containers, err := client.Containers(nsctx)
		if err != nil {
			return err
		}
		for _, container := range containers {
			info, err := container.Info(nsctx)
			if err != nil {
				return err
			}
			if info.Snapshotter != nativeSnapshotter || !retiredNativeOwner(name, info.Labels) {
				continue
			}
			task, err := container.Task(nsctx, nil)
			if errdefs.IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			exit, err := task.Wait(nsctx)
			if err != nil {
				return err
			}
			if err := task.Kill(nsctx, syscall.SIGKILL, containerd.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
			select {
			case status := <-exit:
				if _, _, err := status.Result(); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			if _, err := task.Delete(nsctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

// RemoveRetiredNative runs only while the managed daemon is stopped.
func RemoveRetiredNative(root string) error {
	remaining := false
	err := withNativeMetadata(root, true, func(tx *bolt.Tx) error {
		version := tx.Bucket([]byte("v1"))
		if version == nil {
			return nil
		}
		return version.ForEach(func(name, value []byte) error {
			namespace := version.Bucket(name)
			if namespace == nil {
				return nil
			}
			protected := false
			owned := strings.HasPrefix(string(name), "system.")
			labels := namespace.Bucket([]byte("labels"))
			if labels != nil && string(labels.Get([]byte(namespaceconfig.LabelPrefix+"role"))) == "runtime-internal" {
				owned = true
			}
			namespaceOwned := owned
			containers := namespace.Bucket([]byte("containers"))
			var remove [][]byte
			if containers != nil {
				if err := containers.ForEach(func(id, value []byte) error {
					record := containers.Bucket(id)
					if record == nil || string(record.Get([]byte("snapshotter"))) != nativeSnapshotter {
						return nil
					}
					if retiredNativeOwner(string(name), nativeRecordLabels(record)) {
						owned = true
						remove = append(remove, append([]byte(nil), id...))
					} else {
						protected = true
					}
					return nil
				}); err != nil {
					return err
				}
				for _, id := range remove {
					if err := containers.DeleteBucket(id); err != nil {
						return err
					}
				}
			}
			images := namespace.Bucket([]byte("images"))
			if images != nil {
				if err := images.ForEach(func(id, value []byte) error {
					image := images.Bucket(id)
					if image == nil {
						return nil
					}
					imageLabels := image.Bucket([]byte("labels"))
					if imageLabels == nil || imageLabels.Get([]byte("containerd.io/gc.ref.snapshot.native")) == nil {
						return nil
					}
					if strings.HasPrefix(string(id), "agent-vmm.local/buildkit:") {
						owned = true
					} else if !namespaceOwned {
						protected = true
					}
					return nil
				}); err != nil {
					return err
				}
			}
			if protected {
				remaining = true
			}
			snapshots := namespace.Bucket([]byte("snapshots"))
			if snapshots == nil || snapshots.Bucket([]byte(nativeSnapshotter)) == nil {
				return nil
			}
			if !owned || protected {
				remaining = true
				return nil
			}
			if err := snapshots.DeleteBucket([]byte(nativeSnapshotter)); err != nil {
				return err
			}
			if err := removeNativeReferences(namespace); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	if remaining {
		slog.Default().Info("native snapshot retirement preserved non-VMM-owned references and their cache files")
		return nil
	}
	for _, name := range []string{"native", "io.containerd.snapshotter.v1.native"} {
		if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
			return err
		}
	}
	return nil
}

func nativeRecordLabels(record *bolt.Bucket) map[string]string {
	labels := make(map[string]string)
	if bucket := record.Bucket([]byte("labels")); bucket != nil {
		_ = bucket.ForEach(func(key, value []byte) error {
			if value != nil {
				labels[string(key)] = string(value)
			}
			return nil
		})
	}
	return labels
}

func removeNativeReferences(bucket *bolt.Bucket) error {
	var labels [][]byte
	var children [][]byte
	if err := bucket.ForEach(func(key, value []byte) error {
		if value != nil && strings.HasPrefix(string(key), "containerd.io/gc.ref.snapshot.native") {
			labels = append(labels, append([]byte(nil), key...))
		}
		if value == nil {
			children = append(children, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range labels {
		if err := bucket.Delete(key); err != nil {
			return err
		}
	}
	for _, key := range children {
		child := bucket.Bucket(key)
		if string(key) == "snapshots" && child.Bucket([]byte(nativeSnapshotter)) != nil {
			if err := child.DeleteBucket([]byte(nativeSnapshotter)); err != nil {
				return err
			}
		}
		if err := removeNativeReferences(child); err != nil {
			return err
		}
	}
	return nil
}

func withNativeMetadata(root string, writable bool, run func(*bolt.Tx) error) error {
	path := filepath.Join(root, "io.containerd.metadata.v1.bolt", "meta.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: !writable})
	if err != nil {
		return fmt.Errorf("open stopped containerd metadata: %w", err)
	}
	defer db.Close()
	if writable {
		return db.Update(run)
	}
	return db.View(run)
}
