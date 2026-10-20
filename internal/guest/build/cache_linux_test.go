//go:build linux

package build

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
)

func TestCachePressureReclaimsOnlySettledWriters(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	root := t.TempDir()
	builder := NewBuilder(nil, nil, nil)
	for _, owner := range []string{"active", "locked", "settled"} {
		cache := filepath.Join(root, owner, "buildkit")
		if err := os.MkdirAll(cache, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cache, "download"), []byte(owner), 0600); err != nil {
			t.Fatal(err)
		}
	}
	settled := filepath.Join(root, "settled", "buildkit")
	worker := filepath.Join(settled, "state", "runc-overlayfs", "workerid")
	if err := os.MkdirAll(filepath.Dir(worker), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(worker, []byte("original-worker"), 0600); err != nil {
		t.Fatal(err)
	}
	header := &vmmdv1.BuildImageHeader{RequestId: "before-gc", ActorNamespaceId: "allocation", ControllerCacheScope: "controller"}
	logCacheState(header, settled)
	if !strings.Contains(logs.String(), `controller_scope="controller"`) || !strings.Contains(logs.String(), `worker_exists=true worker_id="original-worker"`) {
		t.Fatalf("entry facts missing: %s", logs.String())
	}
	logs.Reset()
	active := filepath.Join(root, "active", "buildkit")
	locked := filepath.Join(root, "locked", "buildkit")
	builder.cacheActive[active] = 1
	lock := builder.namespaceLock(locked)
	lock.Lock()
	count, err := builder.reclaimCache(context.Background(), root, 1, "storage_headroom_test")
	if err != nil || count != 1 {
		t.Fatalf("GC: %d %v", count, err)
	}
	for _, owner := range []string{"active", "locked"} {
		if _, err := os.Stat(filepath.Join(root, owner, "buildkit", "download")); err != nil {
			t.Fatal("live cache removed", owner, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "settled")); !os.IsNotExist(err) {
		t.Fatal("settled cache retained", err)
	}
	if !strings.Contains(logs.String(), `reason="storage_headroom_test"`) || !strings.Contains(logs.String(), `filesystem_reclaimed_bytes=`) || !strings.Contains(logs.String(), `succeeded=true`) {
		t.Fatalf("GC facts missing: %s", logs.String())
	}
	if strings.Contains(logs.String(), `root="`+filepath.Join(root, "active")+`"`) || strings.Contains(logs.String(), `root="`+filepath.Join(root, "locked")+`"`) {
		t.Fatalf("logged active cache as reclaimed: %s", logs.String())
	}
	logs.Reset()
	logCacheState(header, settled)
	if !strings.Contains(logs.String(), `root_exists=false state_exists=false worker_exists=false worker_id=""`) {
		t.Fatalf("missing post-GC entry facts: %s", logs.String())
	}
	lock.Unlock()
	delete(builder.cacheActive, active)
	count, err = builder.reclaimCache(context.Background(), root, 1, "storage_headroom_test")
	if err != nil || count != 1 {
		t.Fatalf("bounded settled GC: %d %v", count, err)
	}
	owners, err := os.ReadDir(root)
	if err != nil || len(owners) != 1 {
		t.Fatalf("GC exceeded bound: %v %v", owners, err)
	}
	if err := os.MkdirAll(filepath.Dir(worker), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(worker, []byte("rebuilt-worker"), 0600); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	header.RequestId = "after-gc"
	logCacheState(header, settled)
	if !strings.Contains(logs.String(), `request_id="after-gc"`) || !strings.Contains(logs.String(), `root_exists=true state_exists=true worker_exists=true worker_id="rebuilt-worker"`) {
		t.Fatalf("rebuilt entry facts missing: %s", logs.String())
	}

	logs.Reset()
	invalid := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(invalid, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.reclaimCache(context.Background(), invalid, 1, "read_error_test"); err == nil {
		t.Fatal("missing cache inventory error")
	}
	if !strings.Contains(logs.String(), `reason="read_error_test"`) || !strings.Contains(logs.String(), `not a directory`) {
		t.Fatalf("cache inventory error context missing: %s", logs.String())
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := builder.reclaimCache(cancelled, root, 1, "cancelled_test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled GC lost original cause: %v", err)
	}
}

func TestCacheUnlinkLeavesAdmissionFreeIncludingInterruptedCollection(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprint(recovered), func(t *testing.T) {
			root := t.TempDir()
			owner := "cache-owner"
			if recovered {
				owner = ".gc-interrupted"
			}
			cache := filepath.Join(root, owner, "buildkit")
			if err := os.MkdirAll(cache, 0700); err != nil {
				t.Fatal(err)
			}
			const cacheFiles = 15000
			for index := range cacheFiles {
				if err := os.WriteFile(filepath.Join(cache, fmt.Sprint(index)), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			builder := NewBuilder(nil, nil, nil)
			done := make(chan struct{})
			var collectionErr error
			go func() {
				count, err := builder.reclaimCache(context.Background(), root, 1, "concurrent-admission")
				if err == nil && count != 1 {
					err = fmt.Errorf("collected %d roots", count)
				}
				collectionErr = err
				close(done)
			}()
			t.Cleanup(func() { <-done })
			deadline := time.Now().Add(5 * time.Second)
			var unlinkingCache string
			for {
				owners, err := os.ReadDir(root)
				if err != nil {
					t.Fatal(err)
				}
				for _, owner := range owners {
					if !strings.HasPrefix(owner.Name(), ".gc-") {
						continue
					}
					path := filepath.Join(root, owner.Name(), "buildkit")
					entries, err := os.ReadDir(path)
					if err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
					if len(entries) > 0 && len(entries) < cacheFiles {
						unlinkingCache = path
						break
					}
				}
				if unlinkingCache != "" {
					break
				}
				select {
				case <-done:
					t.Fatalf("collection completed before concurrent admission: %v", collectionErr)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("collector did not begin unlinking cache")
				}
				time.Sleep(time.Millisecond)
			}
			// RunArchive and CleanupNamespace resolve owner locks through Builder.mu.
			lock := builder.namespaceLock(filepath.Join(root, "concurrent-owner", "buildkit"))
			lock.Lock()
			lock.Unlock()
			remaining, err := os.Open(unlinkingCache)
			if err != nil {
				t.Fatalf("cache unlink completed before concurrent admission: %v", err)
			}
			entries, readErr := remaining.ReadDir(1)
			closeErr := remaining.Close()
			if readErr != nil || closeErr != nil || len(entries) == 0 {
				t.Fatalf("cache unlink completed before concurrent admission: %v %v", readErr, closeErr)
			}
			select {
			case <-done:
				t.Fatalf("collection completed before concurrent admission: %v", collectionErr)
			default:
			}
			<-done
			if collectionErr != nil {
				t.Fatal(collectionErr)
			}
			if owners, err := os.ReadDir(root); err != nil || len(owners) != 0 {
				t.Fatalf("isolated cache not reclaimed: %v %v", owners, err)
			}
		})
	}
}
