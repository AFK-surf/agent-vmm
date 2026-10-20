//go:build linux

package build

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"golang.org/x/sys/unix"
)

const preparationCacheRoot = "/state/preparation-cache"

// ReclaimCache removes discardable BuildKit state only after every accepted
// workspace using that controller cache has completed its task cleanup.
func (b *Builder) ReclaimCache(ctx context.Context, limit int, reason string) (int, error) {
	return b.reclaimCache(ctx, preparationCacheRoot, limit, reason)
}

func (b *Builder) reclaimCache(ctx context.Context, root string, limit int, reason string) (removed int, reclaimErr error) {
	started := time.Now()
	defer func() {
		log.Printf("BuildKit cache reclaim completed: root=%q reason=%q removed_roots=%d elapsed=%s error=%q", root, reason, removed, time.Since(started), fmt.Sprint(reclaimErr))
	}()
	owners, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	for _, owner := range owners {
		if removed >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if !owner.IsDir() {
			continue
		}
		cache := filepath.Join(root, owner.Name(), "buildkit")
		lock := b.namespaceLock(cache)
		if !lock.TryLock() {
			continue
		}
		b.mu.Lock()
		if b.cacheActive[cache] != 0 {
			b.mu.Unlock()
			lock.Unlock()
			continue
		}
		directory := filepath.Dir(cache)
		if !strings.HasPrefix(owner.Name(), ".gc-") {
			id, idErr := randomID()
			if idErr != nil {
				b.mu.Unlock()
				lock.Unlock()
				return removed, idErr
			}
			isolated := filepath.Join(root, ".gc-"+id)
			err = os.Rename(directory, isolated)
			if errors.Is(err, os.ErrNotExist) {
				b.mu.Unlock()
				lock.Unlock()
				continue
			}
			if err != nil {
				b.mu.Unlock()
				lock.Unlock()
				return removed, err
			}
			directory = isolated
		}
		isolatedCache := filepath.Join(directory, "buildkit")
		b.cacheActive[isolatedCache]++
		b.mu.Unlock()
		var before, after unix.Statfs_t
		beforeErr := unix.Statfs(root, &before)
		err = os.RemoveAll(directory)
		afterErr := unix.Statfs(root, &after)
		freeBefore, freeAfter := int64(before.Bavail)*before.Bsize, int64(after.Bavail)*after.Bsize
		log.Printf("BuildKit cache reclaim: root=%q reason=%q free_bytes_before=%d free_bytes_after=%d filesystem_reclaimed_bytes=%d succeeded=%t error=%q measurement_error=%q", filepath.Dir(cache), reason, freeBefore, freeAfter, freeAfter-freeBefore, err == nil, fmt.Sprint(err), fmt.Sprint(errors.Join(beforeErr, afterErr)))
		b.mu.Lock()
		delete(b.cacheActive, isolatedCache)
		b.mu.Unlock()
		lock.Unlock()
		if err != nil {
			return removed, fmt.Errorf("reclaim BuildKit cache %s (%s): %w", filepath.Dir(cache), reason, err)
		}
		removed++
	}
	return removed, nil
}

// WriterUnsettledError retains the original task failure until exact cleanup
// has proven that the sandbox can no longer write its cache.
type WriterUnsettledError struct{ Err error }

func (e *WriterUnsettledError) Error() string {
	return "builder cache writer is not settled: " + e.Err.Error()
}
func (e *WriterUnsettledError) Unwrap() error { return e.Err }

func logCacheState(header *vmmdv1.BuildImageHeader, root string) {
	state := filepath.Join(root, "state")
	workerRoot := filepath.Join(state, "runc-overlayfs")
	_, rootErr := os.Stat(root)
	_, stateErr := os.Stat(state)
	_, workerErr := os.Stat(workerRoot)
	workerID, readErr := os.ReadFile(filepath.Join(workerRoot, "workerid"))
	log.Printf("BuildKit cache entry: request_id=%q namespace=%q controller_scope=%q root=%q root_exists=%t state_exists=%t worker_exists=%t worker_id=%q observation_error=%q", header.GetRequestId(), header.GetActorNamespaceId(), header.GetControllerCacheScope(), root, rootErr == nil, stateErr == nil, workerErr == nil, string(workerID), fmt.Sprint(errors.Join(rootErr, stateErr, workerErr, readErr)))
}
