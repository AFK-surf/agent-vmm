package ocimaterializer

import (
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
