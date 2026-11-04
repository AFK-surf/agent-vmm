//go:build linux

package control

import (
	"context"
	"time"
)

func (r *Runtime) wakeRuntimeGC() {
	if r.gcWake != nil {
		select {
		case r.gcWake <- struct{}{}:
		default:
		}
	}
}

func (r *Runtime) startRuntimeGC() {
	ctx, cancel := context.WithCancel(context.Background())
	r.gcCancel, r.gcDone, r.gcWake = cancel, make(chan struct{}), make(chan struct{}, 1)
	r.inventory.wakeGC = r.wakeRuntimeGC
	go func() {
		defer close(r.gcDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.gcWake:
			}
			if err := r.collectRuntimeImages(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.logger.Warn("runtime image GC deferred", "error", err)
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				r.wakeRuntimeGC()
			}
		}
	}()
	r.wakeRuntimeGC()
}

func (r *Runtime) stopRuntimeGC() {
	if r.gcCancel != nil {
		r.gcCancel()
		<-r.gcDone
	}
}

// A pass has a fixed upper bound. Changes during the pass coalesce into a new
// wake, including changes to candidates already visited by this pass.
func (r *Runtime) collectRuntimeImages(ctx context.Context) error {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	inventory.mu.RLock()
	last, exists := inventory.runtimeImages.Max()
	remaining := inventory.runtimeImages.Len()
	inventory.mu.RUnlock()
	if !exists {
		return nil
	}
	after := ""
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		references := inventory.runtimeImagePage(after, min(maxGCDeletes, remaining))
		for len(references) > 0 && references[len(references)-1] > last {
			references = references[:len(references)-1]
		}
		if len(references) == 0 {
			return nil
		}
		// Release the slot between pages. Queued materializers keep FIFO priority.
		if err := r.materializationSlot.Acquire(ctx, 1); err != nil {
			return err
		}
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := r.pruneRuntimeImageReferences(batchCtx, references)
		cancel()
		r.materializationSlot.Release(1)
		if err != nil {
			return err
		}
		remaining -= len(references)
		after = references[len(references)-1]
		if after >= last {
			return nil
		}
	}
	return nil
}
