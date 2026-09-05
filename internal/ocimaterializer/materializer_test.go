package ocimaterializer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSameDigestHasSinglePhysicalOwner(t *testing.T) {
	manager := new(Manager)
	var active, maxActive atomic.Int32
	start := make(chan struct{})
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			unlock := manager.Lock("sha256:one")
			current := active.Add(1)
			for {
				old := maxActive.Load()
				if current <= old || maxActive.CompareAndSwap(old, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			unlock()
		}()
	}
	close(start)
	group.Wait()
	if maxActive.Load() != 1 {
		t.Fatalf("same digest materializers=%d", maxActive.Load())
	}
}

func TestDifferentDigestsShareSingleGlobalImportSlot(t *testing.T) {
	manager := new(Manager)
	first := manager.Lock("one")
	done := make(chan struct{})
	go func() { second := manager.Lock("two"); second(); close(done) }()
	select {
	case <-done:
		t.Fatal("different digest bypassed global slot")
	case <-time.After(10 * time.Millisecond):
	}
	first()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("global slot did not advance")
	}
}

func TestImportSlotRejectsUnrelatedDigestButPreservesDigestSingleflight(t *testing.T) {
	manager := new(Manager)
	first, ok := manager.LockImport("one")
	if !ok {
		t.Fatal("first import did not acquire the global slot")
	}
	if unlock, acquired := manager.LockImport("two"); acquired || unlock != nil {
		t.Fatal("unrelated digest waited for or bypassed the occupied import slot")
	}

	done := make(chan bool, 1)
	go func() {
		unlock, acquired := manager.LockImport("one")
		if acquired {
			unlock()
		}
		done <- acquired
	}()
	select {
	case <-done:
		t.Fatal("same digest bypassed its singleflight owner")
	case <-time.After(10 * time.Millisecond):
	}
	first()
	select {
	case acquired := <-done:
		if !acquired {
			t.Fatal("same digest did not advance after its owner completed")
		}
	case <-time.After(time.Second):
		t.Fatal("same digest singleflight did not advance")
	}
}

func TestBuildWaitSharesImportSlotAndReleasesCanceledWaiter(t *testing.T) {
	manager := new(Manager)
	first, ok := manager.LockImport("streamed")
	if !ok {
		t.Fatal("streamed import not admitted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		unlock, err := manager.LockBuild(ctx)
		if unlock != nil {
			unlock()
		}
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter: %v", err)
	}
	first()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unlock, err := manager.LockBuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if other, ok := manager.LockImport("unrelated"); ok {
		other()
		t.Fatal("streaming import bypassed build materialization")
	}
	unlock()
	next, err := manager.LockBuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next()
}
