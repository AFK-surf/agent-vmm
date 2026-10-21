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

func TestImportSlotQueuesInFIFOOrderUntilDeadline(t *testing.T) {
	manager := new(Manager)
	first, err := manager.LockImport(context.Background(), "one")
	if err != nil {
		t.Fatal(err)
	}
	expired, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if unlock, err := manager.LockImport(expired, "two"); !errors.Is(err, context.DeadlineExceeded) || unlock != nil {
		t.Fatalf("occupied slot after deadline: unlock=%v err=%v", unlock != nil, err)
	}

	order := make(chan string, 3)
	var group sync.WaitGroup
	for _, digest := range []string{"two", "three", "one"} {
		group.Add(1)
		go func() {
			defer group.Done()
			unlock, err := manager.LockImport(context.Background(), digest)
			if err != nil {
				t.Error(err)
				return
			}
			order <- digest
			unlock()
		}()
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case digest := <-order:
		t.Fatalf("%s bypassed the occupied slot", digest)
	case <-time.After(10 * time.Millisecond):
	}
	first()
	group.Wait()
	close(order)
	var got []string
	for digest := range order {
		got = append(got, digest)
	}
	if len(got) != 3 || got[0] != "two" || got[1] != "three" || got[2] != "one" {
		t.Fatalf("admission order = %v", got)
	}
}

func TestBuildWaitSharesImportSlotAndReleasesCanceledWaiter(t *testing.T) {
	manager := new(Manager)
	first, err := manager.LockImport(context.Background(), "streamed")
	if err != nil {
		t.Fatal(err)
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
	if _, ok := manager.TryLockBuild(); ok {
		t.Fatal("background materialization bypassed build materialization")
	}
	unlock()
	next, err := manager.LockBuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next()
}
